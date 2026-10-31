use std::{
    future::Future,
    io,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use axum::{
    body::{Body, Bytes},
    extract::{Multipart, Request},
    http::{HeaderMap, StatusCode, header},
};
use futures::{Stream, StreamExt, stream};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

pub const UPLOAD_FIELD_TARGET_ROOT: &str = "target_root";
pub const UPLOAD_FIELD_FILES: &str = "files";
pub const MAX_UPLOAD_FILES: usize = 2_000;
pub const MAX_UPLOAD_FILE_BYTES: usize = 512 * 1024 * 1024;
pub const MAX_UPLOAD_TOTAL_BYTES: usize = 2 * 1024 * 1024 * 1024;
pub const MAX_UPLOAD_TARGET_ROOT_BYTES: usize = 64 * 1024;
pub const MAX_MULTIPART_OVERHEAD_BYTES: usize = 1024 * 1024;
pub const PARTIAL_UPLOAD_ERROR_PREFIX: &str = "PARTIAL_UPLOAD:";
const MULTIPART_ADAPTER_SLICE_BYTES: usize = 16 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MultipartAdapterCommand {
    PayloadChunkConsumed,
    FieldCompleted,
    DrainEpilogue,
}

#[derive(Default)]
struct MultipartOverheadStats {
    overhead_bytes: AtomicUsize,
    parser_overhead_bytes: AtomicUsize,
    exceeded: AtomicBool,
}

pub(crate) struct MultipartOverheadMeter {
    commands: mpsc::UnboundedSender<MultipartAdapterCommand>,
    drain_result: Option<oneshot::Receiver<Result<(), String>>>,
    stats: Arc<MultipartOverheadStats>,
}

impl MultipartOverheadMeter {
    fn payload_chunk_consumed(&self) {
        let _ = self
            .commands
            .send(MultipartAdapterCommand::PayloadChunkConsumed);
    }

    fn field_completed(&self) {
        let _ = self.commands.send(MultipartAdapterCommand::FieldCompleted);
    }

    fn overhead_exceeded(&self) -> bool {
        self.stats.exceeded.load(Ordering::Acquire)
    }

    async fn drain_epilogue(&mut self) -> Result<(), String> {
        self.commands
            .send(MultipartAdapterCommand::DrainEpilogue)
            .map_err(|_| "multipart body adapter stopped before epilogue drain".to_string())?;
        self.drain_result
            .take()
            .expect("epilogue drain is requested only once")
            .await
            .map_err(|_| "multipart body adapter stopped during epilogue drain".to_string())?
    }
}

/// Wrap the raw request body before Axum's existing multipart extractor can
/// buffer it. Invalid/missing multipart Content-Type values are left untouched
/// so Axum retains its existing extractor rejection.
pub(crate) fn meter_multipart_request(
    request: Request,
) -> (Request, Option<MultipartOverheadMeter>) {
    let Some(boundary) = multipart_boundary(request.headers()) else {
        return (request, None);
    };

    let (parts, body) = request.into_parts();
    let (parser_tx, parser_rx) = mpsc::channel(1);
    let (command_tx, command_rx) = mpsc::unbounded_channel();
    let (drain_result_tx, drain_result_rx) = oneshot::channel();
    let stats = Arc::new(MultipartOverheadStats::default());
    let worker_stats = stats.clone();
    drop(tokio::spawn(run_multipart_overhead_adapter(
        body.into_data_stream(),
        boundary,
        parser_tx,
        command_rx,
        drain_result_tx,
        worker_stats,
    )));

    let parser_stream = stream::unfold(parser_rx, |mut receiver| async move {
        receiver.recv().await.map(|item| (item, receiver))
    });
    let request = Request::from_parts(parts, Body::from_stream(parser_stream));

    (
        request,
        Some(MultipartOverheadMeter {
            commands: command_tx,
            drain_result: Some(drain_result_rx),
            stats,
        }),
    )
}

fn multipart_boundary(headers: &HeaderMap) -> Option<String> {
    let content_type = headers.get(header::CONTENT_TYPE)?.to_str().ok()?;
    let content_type = content_type
        .parse::<axum_extra::headers::ContentType>()
        .ok()?
        .to_string();
    let (media_type, parameters) = content_type.split_once(';')?;
    if !media_type
        .trim()
        .eq_ignore_ascii_case("multipart/form-data")
    {
        return None;
    }
    mime_boundary_parameter(parameters)
}

fn mime_boundary_parameter(parameters: &str) -> Option<String> {
    let mut position = 0;
    while position < parameters.len() {
        let rest = &parameters[position..];
        let mut quoted = false;
        let end = rest
            .char_indices()
            .find_map(|(index, character)| match character {
                '"' => {
                    quoted = !quoted;
                    None
                }
                ';' if !quoted => Some(index),
                _ => None,
            })
            .unwrap_or(rest.len());
        let parameter = rest[..end].trim();
        if let Some((name, value)) = parameter.split_once('=')
            && name.trim().eq_ignore_ascii_case("boundary")
        {
            let value = value.trim();
            return if value.starts_with('"') && value.ends_with('"') && value.len() >= 2 {
                Some(value[1..value.len() - 1].to_string())
            } else {
                Some(value.to_string())
            };
        }
        position += end + usize::from(end < rest.len());
    }
    None
}

#[derive(Clone, Copy)]
enum MultipartByteClass {
    Overhead,
    Payload,
}

enum MultipartScanFailure {
    OverheadExceeded,
    BodyRead(String),
    Cancelled,
}

struct MultipartRawScanner<S> {
    source: Pin<Box<S>>,
    frame: Bytes,
    cursor: usize,
    body_eof: bool,
    carry: Vec<u8>,
    parser: mpsc::Sender<Result<Bytes, io::Error>>,
    commands: mpsc::UnboundedReceiver<MultipartAdapterCommand>,
    drain_result: Option<oneshot::Sender<Result<(), String>>>,
    stats: Arc<MultipartOverheadStats>,
}

impl<S> MultipartRawScanner<S>
where
    S: Stream<Item = Result<Bytes, axum::Error>> + Send + 'static,
{
    fn new(
        source: S,
        parser: mpsc::Sender<Result<Bytes, io::Error>>,
        commands: mpsc::UnboundedReceiver<MultipartAdapterCommand>,
        drain_result: oneshot::Sender<Result<(), String>>,
        stats: Arc<MultipartOverheadStats>,
    ) -> Self {
        Self {
            source: Box::pin(source),
            frame: Bytes::new(),
            cursor: 0,
            body_eof: false,
            carry: Vec::new(),
            parser,
            commands,
            drain_result: Some(drain_result),
            stats,
        }
    }

    async fn run(mut self, boundary: String) {
        let first_boundary = [b"--".as_slice(), boundary.as_bytes()].concat();
        let field_boundary = [b"\r\n--".as_slice(), boundary.as_bytes()].concat();
        let outcome = self.process(&first_boundary, &field_boundary).await;
        match outcome {
            Ok(()) | Err(MultipartScanFailure::Cancelled) => {}
            Err(MultipartScanFailure::OverheadExceeded) => {
                self.stats.exceeded.store(true, Ordering::Release);
                let _ = self
                    .parser
                    .send(Err(io::Error::other("multipart overhead exceeds 1 MiB")))
                    .await;
            }
            Err(MultipartScanFailure::BodyRead(message)) => {
                let _ = self.parser.send(Err(io::Error::other(message))).await;
            }
        }
    }

    async fn process(
        &mut self,
        first_boundary: &[u8],
        field_boundary: &[u8],
    ) -> Result<(), MultipartScanFailure> {
        if !self
            .forward_until(first_boundary, MultipartByteClass::Overhead)
            .await?
        {
            return Ok(());
        }

        loop {
            if !self.forward_boundary_suffix().await? {
                return Ok(());
            }
            if !self
                .forward_until(b"\r\n\r\n", MultipartByteClass::Overhead)
                .await?
            {
                return Ok(());
            }
            if !self
                .forward_until(field_boundary, MultipartByteClass::Payload)
                .await?
            {
                return Ok(());
            }

            self.wait_for_command(MultipartAdapterCommand::FieldCompleted)
                .await?;
        }
    }

    async fn ensure_frame(&mut self) -> Result<bool, MultipartScanFailure> {
        loop {
            if self.cursor < self.frame.len() {
                return Ok(true);
            }
            self.frame = Bytes::new();
            self.cursor = 0;
            if self.body_eof {
                return Ok(false);
            }
            match self.next_source_frame().await? {
                Some(Ok(frame)) if frame.is_empty() => {}
                Some(Ok(frame)) => {
                    self.frame = frame;
                    return Ok(true);
                }
                Some(Err(error)) => {
                    return Err(MultipartScanFailure::BodyRead(error.to_string()));
                }
                None => {
                    self.body_eof = true;
                    return Ok(false);
                }
            }
        }
    }

    async fn next_source_frame(
        &mut self,
    ) -> Result<Option<Result<Bytes, axum::Error>>, MultipartScanFailure> {
        tokio::select! {
            biased;
            () = self.parser.closed() => Err(MultipartScanFailure::Cancelled),
            frame = self.source.next() => Ok(frame),
        }
    }

    async fn forward_until(
        &mut self,
        pattern: &[u8],
        prefix_class: MultipartByteClass,
    ) -> Result<bool, MultipartScanFailure> {
        self.carry.clear();
        loop {
            if !self.ensure_frame().await? {
                if !self.carry.is_empty() {
                    let tail = Bytes::copy_from_slice(&self.carry);
                    self.emit_classified(tail, prefix_class).await?;
                    self.carry.clear();
                }
                return Ok(false);
            }

            if self.carry.is_empty() {
                let remaining = &self.frame[self.cursor..];
                if let Some(index) = find_subslice(remaining, pattern) {
                    if index > 0 {
                        let prefix = self.frame.slice(self.cursor..self.cursor + index);
                        self.emit_classified(prefix, prefix_class).await?;
                        self.cursor += index;
                    }
                    let matched = self.frame.slice(self.cursor..self.cursor + pattern.len());
                    self.emit_overhead(matched).await?;
                    self.cursor += pattern.len();
                    return Ok(true);
                }

                let keep = longest_suffix_prefix(remaining, pattern);
                let safe_len = remaining.len() - keep;
                if safe_len > 0 {
                    let safe = self.frame.slice(self.cursor..self.cursor + safe_len);
                    self.emit_classified(safe, prefix_class).await?;
                    self.cursor += safe_len;
                }
                if keep > 0 {
                    self.carry
                        .extend_from_slice(&self.frame[self.cursor..self.frame.len()]);
                }
                self.cursor = self.frame.len();
                continue;
            }

            let carry_len = self.carry.len();
            let take = (self.frame.len() - self.cursor).min(pattern.len());
            let mut probe = self.carry.clone();
            probe.extend_from_slice(&self.frame[self.cursor..self.cursor + take]);
            if let Some(index) = find_subslice(&probe, pattern) {
                if index > 0 {
                    let prefix = Bytes::copy_from_slice(&probe[..index]);
                    self.emit_classified(prefix, prefix_class).await?;
                }
                let end = index + pattern.len();
                let matched = Bytes::copy_from_slice(&probe[index..end]);
                let consumed_from_frame = end.saturating_sub(carry_len);
                self.cursor += consumed_from_frame;
                self.carry.clear();
                self.emit_overhead(matched).await?;
                return Ok(true);
            }

            let keep = longest_suffix_prefix(&probe, pattern);
            let safe_len = probe.len() - keep;
            if safe_len > 0 {
                let safe = Bytes::copy_from_slice(&probe[..safe_len]);
                self.emit_classified(safe, prefix_class).await?;
            }
            self.cursor += take;
            self.carry.clear();
            self.carry.extend_from_slice(&probe[safe_len..]);
        }
    }

    async fn emit_classified(
        &mut self,
        bytes: Bytes,
        class: MultipartByteClass,
    ) -> Result<(), MultipartScanFailure> {
        match class {
            MultipartByteClass::Overhead => self.emit_overhead(bytes).await,
            MultipartByteClass::Payload => {
                let mut offset = 0;
                while offset < bytes.len() {
                    let end = (offset + MULTIPART_ADAPTER_SLICE_BYTES).min(bytes.len());
                    self.parser
                        .send(Ok(bytes.slice(offset..end)))
                        .await
                        .map_err(|_| MultipartScanFailure::Cancelled)?;
                    self.wait_for_command(MultipartAdapterCommand::PayloadChunkConsumed)
                        .await?;
                    offset = end;
                }
                Ok(())
            }
        }
    }

    async fn emit_overhead(&mut self, bytes: Bytes) -> Result<(), MultipartScanFailure> {
        if bytes.is_empty() {
            return Ok(());
        }
        let mut offset = 0;
        while offset < bytes.len() {
            let counted = self.stats.overhead_bytes.load(Ordering::Acquire);
            let remaining = MAX_MULTIPART_OVERHEAD_BYTES.saturating_sub(counted);
            let permitted = (bytes.len() - offset)
                .min(remaining)
                .min(MULTIPART_ADAPTER_SLICE_BYTES);
            if permitted == 0 {
                self.stats.exceeded.store(true, Ordering::Release);
                return Err(MultipartScanFailure::OverheadExceeded);
            }

            let end = offset + permitted;
            self.stats
                .overhead_bytes
                .fetch_add(permitted, Ordering::AcqRel);
            self.stats
                .parser_overhead_bytes
                .fetch_add(permitted, Ordering::AcqRel);
            self.parser
                .send(Ok(bytes.slice(offset..end)))
                .await
                .map_err(|_| MultipartScanFailure::Cancelled)?;
            offset = end;
        }
        Ok(())
    }

    async fn forward_boundary_suffix(&mut self) -> Result<bool, MultipartScanFailure> {
        let first_two = match self.read_up_to(2).await {
            Ok(bytes) => bytes,
            Err((bytes, message)) => {
                self.emit_overhead(Bytes::from(bytes)).await?;
                return Err(MultipartScanFailure::BodyRead(message));
            }
        };
        if first_two == b"--" {
            self.emit_overhead(Bytes::from(first_two)).await?;
            self.wait_for_command(MultipartAdapterCommand::DrainEpilogue)
                .await?;
            let result = self.drain_epilogue().await;
            if let Some(sender) = self.drain_result.take() {
                let _ = sender.send(result);
            }
            return Ok(false);
        }

        let mut separator = Vec::with_capacity(2);
        let mut padding = true;
        for byte in first_two {
            if padding && matches!(byte, b' ' | b'\t') {
                self.emit_overhead(Bytes::from(vec![byte])).await?;
            } else {
                padding = false;
                separator.push(byte);
                if separator.len() == 2 {
                    break;
                }
            }
        }

        while separator.len() < 2 {
            let next = match self.read_up_to(1).await {
                Ok(bytes) => bytes,
                Err((bytes, message)) => {
                    self.emit_overhead(Bytes::from(bytes)).await?;
                    self.emit_overhead(Bytes::from(separator)).await?;
                    return Err(MultipartScanFailure::BodyRead(message));
                }
            };
            let Some(byte) = next.first().copied() else {
                self.emit_overhead(Bytes::from(separator)).await?;
                return Ok(false);
            };
            if padding && matches!(byte, b' ' | b'\t') {
                self.emit_overhead(Bytes::from(vec![byte])).await?;
            } else {
                padding = false;
                separator.push(byte);
            }
        }

        let is_crlf = separator.as_slice() == b"\r\n";
        self.emit_overhead(Bytes::from(separator)).await?;
        Ok(is_crlf)
    }

    async fn read_up_to(&mut self, limit: usize) -> Result<Vec<u8>, (Vec<u8>, String)> {
        let mut bytes = Vec::with_capacity(limit);
        while bytes.len() < limit {
            if self.cursor < self.frame.len() {
                let take = (limit - bytes.len()).min(self.frame.len() - self.cursor);
                bytes.extend_from_slice(&self.frame[self.cursor..self.cursor + take]);
                self.cursor += take;
                continue;
            }
            self.frame = Bytes::new();
            self.cursor = 0;
            if self.body_eof {
                break;
            }
            let next_frame = match self.next_source_frame().await {
                Ok(frame) => frame,
                Err(MultipartScanFailure::BodyRead(message)) => return Err((bytes, message)),
                Err(MultipartScanFailure::OverheadExceeded) => {
                    return Err((bytes, "multipart overhead exceeds 1 MiB".to_string()));
                }
                Err(MultipartScanFailure::Cancelled) => {
                    return Err((bytes, "multipart body adapter cancelled".to_string()));
                }
            };
            match next_frame {
                Some(Ok(frame)) if frame.is_empty() => {}
                Some(Ok(frame)) => self.frame = frame,
                Some(Err(error)) => return Err((bytes, error.to_string())),
                None => self.body_eof = true,
            }
        }
        Ok(bytes)
    }

    async fn wait_for_command(
        &mut self,
        expected: MultipartAdapterCommand,
    ) -> Result<(), MultipartScanFailure> {
        loop {
            tokio::select! {
                command = self.commands.recv() => match command {
                    Some(command) if command == expected => return Ok(()),
                    Some(MultipartAdapterCommand::PayloadChunkConsumed)
                        if expected != MultipartAdapterCommand::PayloadChunkConsumed => {}
                    Some(_) => {}
                    None => return Err(MultipartScanFailure::Cancelled),
                },
                () = self.parser.closed() => return Err(MultipartScanFailure::Cancelled),
            }
        }
    }

    async fn drain_epilogue(&mut self) -> Result<(), String> {
        let mut body_error = None;
        self.count_drained_bytes(self.frame.len().saturating_sub(self.cursor));
        self.cursor = self.frame.len();

        while !self.body_eof {
            let frame = tokio::select! {
                biased;
                command = self.commands.recv() => {
                    if command.is_none() {
                        return Err("multipart epilogue drain cancelled".to_string());
                    }
                    continue;
                }
                frame = self.source.next() => frame,
            };
            match frame {
                Some(Ok(frame)) => self.count_drained_bytes(frame.len()),
                Some(Err(error)) => {
                    body_error = Some(error.to_string());
                    break;
                }
                None => self.body_eof = true,
            }
        }

        if self.stats.exceeded.load(Ordering::Acquire) {
            Err("multipart overhead exceeds 1 MiB".to_string())
        } else if let Some(error) = body_error {
            Err(format!("failed while draining multipart epilogue: {error}"))
        } else {
            Ok(())
        }
    }

    fn count_drained_bytes(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let counted = self.stats.overhead_bytes.load(Ordering::Acquire);
        let remaining = MAX_MULTIPART_OVERHEAD_BYTES.saturating_sub(counted);
        if bytes > remaining {
            self.stats
                .overhead_bytes
                .store(MAX_MULTIPART_OVERHEAD_BYTES, Ordering::Release);
            self.stats.exceeded.store(true, Ordering::Release);
        } else {
            self.stats.overhead_bytes.fetch_add(bytes, Ordering::AcqRel);
        }
    }
}

async fn run_multipart_overhead_adapter<S>(
    source: S,
    boundary: String,
    parser: mpsc::Sender<Result<Bytes, io::Error>>,
    commands: mpsc::UnboundedReceiver<MultipartAdapterCommand>,
    drain_result: oneshot::Sender<Result<(), String>>,
    stats: Arc<MultipartOverheadStats>,
) where
    S: Stream<Item = Result<Bytes, axum::Error>> + Send + 'static,
{
    MultipartRawScanner::new(source, parser, commands, drain_result, stats)
        .run(boundary)
        .await;
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    if needle.len() > haystack.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn longest_suffix_prefix(bytes: &[u8], pattern: &[u8]) -> usize {
    (1..=bytes.len().min(pattern.len().saturating_sub(1)))
        .rev()
        .find(|length| bytes[bytes.len() - length..] == pattern[..*length])
        .unwrap_or(0)
}

#[derive(Debug, Clone)]
pub struct UploadedFiles {
    pub files: Vec<String>,
}

impl UploadedFiles {
    #[must_use]
    pub fn uploaded_count(&self) -> usize {
        self.files.len()
    }
}

#[derive(Default)]
struct UploadSession {
    target_root: String,
    total_bytes: usize,
    file_count: usize,
    uploaded_files: Vec<String>,
    seen_file_fields: bool,
    seen_target_root: bool,
}

struct UploadTempGuard {
    path: PathBuf,
    #[cfg(test)]
    cleanup_complete: Option<oneshot::Sender<()>>,
}

// A blocking create may finish after the request future has been cancelled.
// Keep the exclusive file and its path guard together until the request can
// take ownership, so a detached task's result drop closes the file before the
// guard removes only the stage it created.
struct UploadTempAcquisition {
    guard: Option<UploadTempGuard>,
    file: Option<std::fs::File>,
}

impl UploadTempAcquisition {
    fn new(guard: UploadTempGuard, file: std::fs::File) -> Self {
        Self {
            guard: Some(guard),
            file: Some(file),
        }
    }

    fn into_parts(mut self) -> (UploadTempGuard, std::fs::File) {
        (
            self.guard.take().expect("acquired upload temp has a guard"),
            self.file.take().expect("acquired upload temp has a file"),
        )
    }
}

impl Drop for UploadTempAcquisition {
    fn drop(&mut self) {
        // On platforms such as Windows, close the open handle before unlinking.
        drop(self.file.take());
        drop(self.guard.take());
    }
}

#[cfg(test)]
struct UploadTempAcquisitionTestHook {
    acquired: oneshot::Sender<PathBuf>,
    release: std::sync::mpsc::Receiver<()>,
    cleanup_complete: oneshot::Sender<()>,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UploadIoOperation {
    Write,
    Flush,
    PublishNoClobber,
}

// The operation futures stay `Send` so the normal handler future remains spawnable.
#[allow(clippy::manual_async_fn)]
trait UploadIo: Sync {
    fn write_all<'a>(
        &'a self,
        relative_path: &'a str,
        output: &'a mut tokio::fs::File,
        bytes: &'a [u8],
    ) -> impl Future<Output = io::Result<()>> + Send + 'a;

    fn flush<'a>(
        &'a self,
        relative_path: &'a str,
        output: &'a mut tokio::fs::File,
    ) -> impl Future<Output = io::Result<()>> + Send + 'a;

    fn publish_no_clobber<'a>(
        &'a self,
        relative_path: &'a str,
        stage_path: &'a Path,
        destination: &'a Path,
    ) -> impl Future<Output = io::Result<()>> + Send + 'a;
}

struct TokioUploadIo;

#[allow(clippy::manual_async_fn)]
impl UploadIo for TokioUploadIo {
    fn write_all<'a>(
        &'a self,
        _relative_path: &'a str,
        output: &'a mut tokio::fs::File,
        bytes: &'a [u8],
    ) -> impl Future<Output = io::Result<()>> + Send + 'a {
        async move { output.write_all(bytes).await }
    }

    fn flush<'a>(
        &'a self,
        _relative_path: &'a str,
        output: &'a mut tokio::fs::File,
    ) -> impl Future<Output = io::Result<()>> + Send + 'a {
        async move { output.flush().await }
    }

    fn publish_no_clobber<'a>(
        &'a self,
        _relative_path: &'a str,
        stage_path: &'a Path,
        destination: &'a Path,
    ) -> impl Future<Output = io::Result<()>> + Send + 'a {
        async move { tokio::fs::hard_link(stage_path, destination).await }
    }
}

#[cfg(test)]
struct FailingUploadIo {
    operation: UploadIoOperation,
    relative_path: String,
    failure_was_injected: AtomicBool,
    injected_write_bytes: AtomicUsize,
}

#[cfg(test)]
impl FailingUploadIo {
    fn failing_at(operation: UploadIoOperation, relative_path: &str) -> Self {
        Self {
            operation,
            relative_path: relative_path.to_string(),
            failure_was_injected: AtomicBool::new(false),
            injected_write_bytes: AtomicUsize::new(0),
        }
    }

    fn failure_was_injected(&self) -> bool {
        self.failure_was_injected.load(Ordering::Relaxed)
    }

    fn injected_write_bytes(&self) -> usize {
        self.injected_write_bytes.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
#[allow(clippy::manual_async_fn)]
impl UploadIo for FailingUploadIo {
    fn write_all<'a>(
        &'a self,
        relative_path: &'a str,
        output: &'a mut tokio::fs::File,
        bytes: &'a [u8],
    ) -> impl Future<Output = io::Result<()>> + Send + 'a {
        async move {
            if self.operation == UploadIoOperation::Write
                && self.relative_path == relative_path
                && !bytes.is_empty()
            {
                let write_bytes = bytes.len().div_ceil(2);
                output.write_all(&bytes[..write_bytes]).await?;
                self.injected_write_bytes
                    .store(write_bytes, Ordering::Relaxed);
                self.failure_was_injected.store(true, Ordering::Relaxed);
                return Err(io::Error::other("injected write failure"));
            }
            output.write_all(bytes).await
        }
    }

    fn flush<'a>(
        &'a self,
        relative_path: &'a str,
        output: &'a mut tokio::fs::File,
    ) -> impl Future<Output = io::Result<()>> + Send + 'a {
        async move {
            output.flush().await?;
            if self.operation == UploadIoOperation::Flush && self.relative_path == relative_path {
                self.failure_was_injected.store(true, Ordering::Relaxed);
                return Err(io::Error::other("injected flush failure"));
            }
            Ok(())
        }
    }

    fn publish_no_clobber<'a>(
        &'a self,
        relative_path: &'a str,
        stage_path: &'a Path,
        destination: &'a Path,
    ) -> impl Future<Output = io::Result<()>> + Send + 'a {
        async move {
            if self.operation == UploadIoOperation::PublishNoClobber
                && self.relative_path == relative_path
            {
                self.failure_was_injected.store(true, Ordering::Relaxed);
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "injected no-clobber publication failure",
                ));
            }
            tokio::fs::hard_link(stage_path, destination).await
        }
    }
}

impl UploadTempGuard {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            #[cfg(test)]
            cleanup_complete: None,
        }
    }
}

impl Drop for UploadTempGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        #[cfg(test)]
        if let Some(cleanup_complete) = self.cleanup_complete.take() {
            let _ = cleanup_complete.send(());
        }
    }
}

pub fn build_upload_relative_path(
    target_root: &str,
    client_file_name: &str,
) -> std::result::Result<String, (StatusCode, String)> {
    let mut segments = split_upload_segments(target_root, true)?;
    segments.extend(split_upload_segments(client_file_name, false)?);

    if segments.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "Upload path resolved to empty".to_string(),
        ));
    }

    Ok(segments.join("/"))
}

pub fn normalize_upload_path(
    raw: &str,
    allow_empty: bool,
) -> std::result::Result<String, (StatusCode, String)> {
    let segments = split_upload_segments(raw, allow_empty)?;
    Ok(segments.join("/"))
}

pub fn split_upload_segments(
    raw: &str,
    allow_empty: bool,
) -> std::result::Result<Vec<String>, (StatusCode, String)> {
    let normalized = raw.trim().replace('\\', "/");

    if normalized.is_empty() {
        if allow_empty {
            return Ok(Vec::new());
        }
        return Err((StatusCode::BAD_REQUEST, "Upload path is empty".to_string()));
    }

    if normalized.starts_with('/') {
        return Err((
            StatusCode::BAD_REQUEST,
            "Absolute upload paths are not allowed".to_string(),
        ));
    }

    let mut segments = Vec::new();
    for segment in normalized.split('/') {
        let part = segment.trim();
        if part.is_empty() || part == "." {
            continue;
        }
        if part == ".." {
            return Err((
                StatusCode::BAD_REQUEST,
                "Upload path contains parent traversal ('..')".to_string(),
            ));
        }
        if part.contains('\0') {
            return Err((
                StatusCode::BAD_REQUEST,
                "Upload path contains NUL byte".to_string(),
            ));
        }
        if part.contains(':') {
            return Err((
                StatusCode::BAD_REQUEST,
                "Upload path contains invalid ':' character".to_string(),
            ));
        }
        segments.push(part.to_string());
    }

    if !allow_empty && segments.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "Upload path is empty".to_string()));
    }

    Ok(segments)
}

/// Save uploaded image files from multipart payload into `data_dir`.
///
/// Multipart fields:
/// - `target_root` (optional text field, must appear before files)
/// - `files` (one or more file fields)
///
/// The `is_supported` callback is used to validate file extensions/types.
pub async fn save_uploaded_images<F>(
    data_dir: &Path,
    multipart: Multipart,
    is_supported: F,
) -> std::result::Result<UploadedFiles, (StatusCode, String)>
where
    F: Fn(&Path) -> bool,
{
    save_uploaded_images_with_io_and_meter(data_dir, multipart, is_supported, &TokioUploadIo, None)
        .await
}

pub(crate) async fn save_uploaded_images_with_overhead<F>(
    data_dir: &Path,
    multipart: Multipart,
    is_supported: F,
    mut meter: MultipartOverheadMeter,
) -> std::result::Result<UploadedFiles, (StatusCode, String)>
where
    F: Fn(&Path) -> bool,
{
    save_uploaded_images_with_io_and_meter(
        data_dir,
        multipart,
        is_supported,
        &TokioUploadIo,
        Some(&mut meter),
    )
    .await
}

#[cfg(test)]
async fn save_uploaded_images_with_io<F, I>(
    data_dir: &Path,
    multipart: Multipart,
    is_supported: F,
    io: &I,
) -> std::result::Result<UploadedFiles, (StatusCode, String)>
where
    F: Fn(&Path) -> bool,
    I: UploadIo,
{
    save_uploaded_images_with_io_and_meter(data_dir, multipart, is_supported, io, None).await
}

async fn save_uploaded_images_with_io_and_meter<F, I>(
    data_dir: &Path,
    mut multipart: Multipart,
    is_supported: F,
    io: &I,
    meter: Option<&mut MultipartOverheadMeter>,
) -> std::result::Result<UploadedFiles, (StatusCode, String)>
where
    F: Fn(&Path) -> bool,
    I: UploadIo,
{
    let mut session = UploadSession::default();

    loop {
        let mut field = match multipart.next_field().await {
            Ok(Some(field)) => field,
            Ok(None) => break,
            Err(err) => {
                let error = multipart_read_error(
                    format!("Invalid multipart payload: {err}"),
                    meter.as_deref(),
                );
                return Err(partial_upload_error(error, &session));
            }
        };

        match field.name() {
            Some(UPLOAD_FIELD_TARGET_ROOT) => {
                if session.seen_target_root {
                    return Err(partial_upload_error(
                        (
                            StatusCode::BAD_REQUEST,
                            "Only one 'target_root' field is allowed".to_string(),
                        ),
                        &session,
                    ));
                }
                session.seen_target_root = true;
                session.target_root = parse_target_root_field_with_meter(
                    field,
                    session.seen_file_fields,
                    meter.as_deref(),
                )
                .await
                .map_err(|err| partial_upload_error(err, &session))?;
                if let Some(meter) = meter.as_deref() {
                    meter.field_completed();
                }
            }
            Some(UPLOAD_FIELD_FILES) => {
                session.seen_file_fields = true;
                session.file_count = session.file_count.saturating_add(1);
                enforce_file_count_limit(session.file_count)
                    .map_err(|err| partial_upload_error(err, &session))?;
                let relative_path = save_uploaded_file_field(
                    data_dir,
                    &mut field,
                    &is_supported,
                    &mut session,
                    io,
                    meter.as_deref(),
                )
                .await
                .map_err(|err| partial_upload_error(err, &session))?;
                session.uploaded_files.push(relative_path);
                if let Some(meter) = meter.as_deref() {
                    meter.field_completed();
                }
            }
            Some(name) => {
                return Err(partial_upload_error(
                    (
                        StatusCode::BAD_REQUEST,
                        format!("Unexpected multipart field '{name}'"),
                    ),
                    &session,
                ));
            }
            None => {
                while let Some(chunk) = field.chunk().await.map_err(|err| {
                    partial_upload_error(
                        multipart_read_error(
                            format!("Failed to read unnamed multipart field: {err}"),
                            meter.as_deref(),
                        ),
                        &session,
                    )
                })? {
                    if let Some(meter) = meter.as_deref() {
                        meter.payload_chunk_consumed();
                    }
                    drop(chunk);
                }
                if let Some(meter) = meter.as_deref() {
                    meter.field_completed();
                }
            }
        }
    }

    if let Some(meter) = meter
        && let Err(message) = meter.drain_epilogue().await
    {
        let error = if meter.overhead_exceeded() {
            multipart_overhead_error()
        } else {
            (StatusCode::BAD_REQUEST, message)
        };
        return Err(partial_upload_error(error, &session));
    }

    if session.uploaded_files.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "No files were uploaded".to_string(),
        ));
    }

    Ok(UploadedFiles {
        files: session.uploaded_files,
    })
}

fn multipart_overhead_error() -> (StatusCode, String) {
    (
        StatusCode::PAYLOAD_TOO_LARGE,
        format!(
            "Multipart body overhead exceeds the maximum of {MAX_MULTIPART_OVERHEAD_BYTES} bytes"
        ),
    )
}

fn multipart_read_error(
    fallback_message: String,
    meter: Option<&MultipartOverheadMeter>,
) -> (StatusCode, String) {
    if meter.is_some_and(MultipartOverheadMeter::overhead_exceeded) {
        multipart_overhead_error()
    } else {
        (StatusCode::BAD_REQUEST, fallback_message)
    }
}

#[cfg(test)]
async fn parse_target_root_field(
    field: axum::extract::multipart::Field<'_>,
    seen_file_fields: bool,
) -> std::result::Result<String, (StatusCode, String)> {
    parse_target_root_field_with_meter(field, seen_file_fields, None).await
}

async fn parse_target_root_field_with_meter(
    field: axum::extract::multipart::Field<'_>,
    seen_file_fields: bool,
    meter: Option<&MultipartOverheadMeter>,
) -> std::result::Result<String, (StatusCode, String)> {
    if seen_file_fields {
        return Err((
            StatusCode::BAD_REQUEST,
            "'target_root' must appear before file fields".to_string(),
        ));
    }

    let mut raw_target = Vec::new();
    let mut field = field;
    while let Some(chunk) = field
        .chunk()
        .await
        .map_err(|err| multipart_read_error(format!("Failed to read target_root: {err}"), meter))?
    {
        if raw_target.len().saturating_add(chunk.len()) > MAX_UPLOAD_TARGET_ROOT_BYTES {
            return Err((
                StatusCode::PAYLOAD_TOO_LARGE,
                format!(
                    "'target_root' exceeds the maximum of {} bytes",
                    MAX_UPLOAD_TARGET_ROOT_BYTES
                ),
            ));
        }
        raw_target.extend_from_slice(&chunk);
        if let Some(meter) = meter {
            meter.payload_chunk_consumed();
        }
    }
    let raw_target = String::from_utf8(raw_target).map_err(|err| {
        (
            StatusCode::BAD_REQUEST,
            format!("Invalid UTF-8 in target_root: {err}"),
        )
    })?;
    normalize_upload_path(&raw_target, true)
}

fn partial_upload_error(
    (status, message): (StatusCode, String),
    session: &UploadSession,
) -> (StatusCode, String) {
    if session.uploaded_files.is_empty() {
        return (status, message);
    }

    (
        status,
        format!(
            "{PARTIAL_UPLOAD_ERROR_PREFIX} {} file(s) were kept; remaining upload failed: {message}",
            session.uploaded_files.len()
        ),
    )
}

fn enforce_file_count_limit(file_count: usize) -> std::result::Result<(), (StatusCode, String)> {
    if file_count > MAX_UPLOAD_FILES {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            format!("Upload exceeds maximum of {} files", MAX_UPLOAD_FILES),
        ));
    }
    Ok(())
}

async fn save_uploaded_file_field<F, I>(
    data_dir: &Path,
    field: &mut axum::extract::multipart::Field<'_>,
    is_supported: &F,
    session: &mut UploadSession,
    io: &I,
    meter: Option<&MultipartOverheadMeter>,
) -> std::result::Result<String, (StatusCode, String)>
where
    F: Fn(&Path) -> bool,
    I: UploadIo,
{
    let client_file_name = field.file_name().ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            "Each uploaded file must include a filename".to_string(),
        )
    })?;
    let relative_path = build_upload_relative_path(&session.target_root, client_file_name)?;
    let destination = data_dir.join(&relative_path);

    if !is_supported(&destination) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("Unsupported file type: {}", relative_path),
        ));
    }

    let (temp_guard, mut output) = create_upload_temp_output(&destination, &relative_path).await?;
    let temp_path = temp_guard.path.as_path();

    let file_bytes = stream_upload_chunks(
        field,
        &relative_path,
        &mut output,
        &mut session.total_bytes,
        io,
        meter,
    )
    .await?;
    let stored_relative_path = finalize_upload_file(
        file_bytes,
        &relative_path,
        temp_path,
        &destination,
        output,
        io,
    )
    .await?;
    // Hard-link publication leaves the private stage name in place. Dropping
    // only its owning guard unlinks that stage while preserving the destination.
    drop(temp_guard);

    Ok(stored_relative_path)
}

async fn create_upload_temp_output(
    destination: &Path,
    relative_path: &str,
) -> std::result::Result<(UploadTempGuard, tokio::fs::File), (StatusCode, String)> {
    #[cfg(test)]
    {
        create_upload_temp_output_inner(destination, relative_path, None).await
    }
    #[cfg(not(test))]
    {
        create_upload_temp_output_inner(destination, relative_path).await
    }
}

#[cfg(test)]
async fn create_upload_temp_output_with_test_hook(
    destination: &Path,
    relative_path: &str,
    test_hook: UploadTempAcquisitionTestHook,
) -> std::result::Result<(UploadTempGuard, tokio::fs::File), (StatusCode, String)> {
    create_upload_temp_output_inner(destination, relative_path, Some(test_hook)).await
}

async fn create_upload_temp_output_inner(
    destination: &Path,
    relative_path: &str,
    #[cfg(test)] test_hook: Option<UploadTempAcquisitionTestHook>,
) -> std::result::Result<(UploadTempGuard, tokio::fs::File), (StatusCode, String)> {
    let parent = destination.parent().ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            format!("Invalid destination path: {}", relative_path),
        )
    })?;
    tokio::fs::create_dir_all(parent).await.map_err(|err| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to create upload directory: {err}"),
        )
    })?;

    let parent = parent.to_path_buf();
    let file_name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("upload")
        .to_string();
    let acquired = tokio::task::spawn_blocking(move || {
        const MAX_STAGE_CREATE_ATTEMPTS: usize = 8;
        #[cfg(test)]
        let mut test_hook = test_hook;
        let mut last_collision = None;
        for _ in 0..MAX_STAGE_CREATE_ATTEMPTS {
            let temp_path = parent.join(format!(".{file_name}.uploading-{}", Uuid::new_v4()));
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp_path)
            {
                Ok(file) => {
                    let acquired = UploadTempAcquisition::new(UploadTempGuard::new(temp_path), file);
                    #[cfg(test)]
                    let acquired = {
                        let mut acquired = acquired;
                        if let Some(test_hook) = test_hook.take() {
                            let guard = acquired
                                .guard
                                .as_mut()
                                .expect("acquired upload temp has a guard");
                            guard.cleanup_complete = Some(test_hook.cleanup_complete);
                            let _ = test_hook.acquired.send(guard.path.clone());
                            let _ = test_hook.release.recv();
                        }
                        acquired
                    };
                    return Ok(acquired);
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    last_collision = Some(error);
                }
                Err(error) => return Err(error),
            }
        }

        Err(last_collision.expect("an exhausted exclusive-temp loop has a collision"))
    })
    .await
    .map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!(
                "Failed to create exclusive upload temp for '{relative_path}': blocking task failed: {error}"
            ),
        )
    })?
    .map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to create exclusive upload temp for '{relative_path}': {error}"),
        )
    })?;

    let (guard, file) = acquired.into_parts();
    Ok((guard, tokio::fs::File::from_std(file)))
}

async fn stream_upload_chunks<I: UploadIo>(
    field: &mut axum::extract::multipart::Field<'_>,
    relative_path: &str,
    output: &mut tokio::fs::File,
    total_bytes: &mut usize,
    io: &I,
    meter: Option<&MultipartOverheadMeter>,
) -> std::result::Result<usize, (StatusCode, String)> {
    let mut file_bytes = 0usize;
    while let Some(chunk) = field.chunk().await.map_err(|err| {
        multipart_read_error(
            format!("Failed to read upload chunk for '{}': {err}", relative_path),
            meter,
        )
    })? {
        file_bytes = file_bytes.saturating_add(chunk.len());
        *total_bytes = total_bytes.saturating_add(chunk.len());

        if file_bytes > MAX_UPLOAD_FILE_BYTES {
            return Err((
                StatusCode::PAYLOAD_TOO_LARGE,
                format!(
                    "File '{}' exceeds per-file limit ({} bytes)",
                    relative_path, MAX_UPLOAD_FILE_BYTES
                ),
            ));
        }
        if *total_bytes > MAX_UPLOAD_TOTAL_BYTES {
            return Err((
                StatusCode::PAYLOAD_TOO_LARGE,
                format!(
                    "Upload exceeds total limit ({} bytes)",
                    MAX_UPLOAD_TOTAL_BYTES
                ),
            ));
        }

        io.write_all(relative_path, output, &chunk)
            .await
            .map_err(|err| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed writing upload '{}': {err}", relative_path),
                )
            })?;
        if let Some(meter) = meter {
            meter.payload_chunk_consumed();
        }
    }
    Ok(file_bytes)
}

async fn finalize_upload_file<I: UploadIo>(
    file_bytes: usize,
    relative_path: &str,
    temp_path: &Path,
    destination: &Path,
    mut output: tokio::fs::File,
    io: &I,
) -> std::result::Result<String, (StatusCode, String)> {
    if file_bytes == 0 {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("Uploaded file '{}' is empty", relative_path),
        ));
    }

    io.flush(relative_path, &mut output).await.map_err(|err| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to flush upload '{}': {err}", relative_path),
        )
    })?;
    drop(output);

    let destination_parent = destination.parent().ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            format!("Invalid destination path: {relative_path}"),
        )
    })?;
    let mut candidate_relative_path = relative_path.to_string();
    let mut candidate_destination = destination.to_path_buf();
    let mut next_version = Some(2_u64);

    loop {
        match io
            .publish_no_clobber(&candidate_relative_path, temp_path, &candidate_destination)
            .await
        {
            Ok(()) => return Ok(candidate_relative_path),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let version = next_version.ok_or_else(|| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("No available version suffix remains for upload '{relative_path}'"),
                    )
                })?;
                candidate_relative_path = versioned_upload_relative_path(relative_path, version)
                    .ok_or_else(|| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("Unable to version supported upload path '{relative_path}'"),
                        )
                    })?;
                next_version = version.checked_add(1);
                let candidate_file_name = Path::new(&candidate_relative_path)
                    .file_name()
                    .ok_or_else(|| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("Invalid versioned upload path '{candidate_relative_path}'"),
                        )
                    })?;
                candidate_destination = destination_parent.join(candidate_file_name);
            }
            Err(error) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!(
                        "Failed no-clobber publication of upload '{candidate_relative_path}' (hard-link support may be unavailable; no replacing-rename fallback was attempted): {error}"
                    ),
                ));
            }
        }
    }
}

fn versioned_upload_relative_path(relative_path: &str, version: u64) -> Option<String> {
    let (parent, leaf) = relative_path
        .rsplit_once('/')
        .map_or(("", relative_path), |(parent, leaf)| (parent, leaf));
    let (stem, extension) = leaf.rsplit_once('.')?;
    let versioned_leaf = format!("{stem}_v{version}.{extension}");
    if parent.is_empty() {
        Some(versioned_leaf)
    } else {
        Some(format!("{parent}/{versioned_leaf}"))
    }
}

#[cfg(test)]
#[path = "upload.test.rs"]
mod tests;

use std::fmt;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::{Duration, Instant};

use agent_knowledge_core::{ErrorCode, PathAttestation, PathAttestationError, RequestId};
use agent_knowledge_protocol::{
    ClientId, ProtocolErrorResponse, RequestStatus, StatusRequest, StatusResponse, SubmitResponse,
};
use agent_knowledge_queue::{FileQueue, PackagePolicy, QueueReader, QueueRequestStatus};
use serde::{Deserialize, Serialize};

use crate::{GatewayError, submit};

const CURRENT_INGRESS_PROTOCOL_VERSION: u16 = 1;
const MAXIMUM_HEADER_BYTES: u64 = 4 * 1024;
const MAXIMUM_RESPONSE_BYTES: u64 = 64 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum IngressRequest {
    Submit {
        protocol_version: u16,
        client_id: ClientId,
    },
    Status {
        protocol_version: u16,
        request_id: RequestId,
    },
}

impl IngressRequest {
    const fn protocol_version(&self) -> u16 {
        match self {
            Self::Submit {
                protocol_version, ..
            }
            | Self::Status {
                protocol_version, ..
            } => *protocol_version,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
enum IngressResponse {
    Submit {
        protocol_version: u16,
        response: SubmitResponse,
    },
    Status {
        protocol_version: u16,
        response: StatusResponse,
    },
    Error {
        protocol_version: u16,
        error: ProtocolErrorResponse,
    },
}

impl IngressResponse {
    fn submit(response: SubmitResponse) -> Self {
        Self::Submit {
            protocol_version: CURRENT_INGRESS_PROTOCOL_VERSION,
            response,
        }
    }

    fn status(response: StatusResponse) -> Self {
        Self::Status {
            protocol_version: CURRENT_INGRESS_PROTOCOL_VERSION,
            response,
        }
    }

    fn error(error_code: ErrorCode) -> Self {
        Self::Error {
            protocol_version: CURRENT_INGRESS_PROTOCOL_VERSION,
            error: ProtocolErrorResponse::new(error_code),
        }
    }

    const fn protocol_version(&self) -> u16 {
        match self {
            Self::Submit {
                protocol_version, ..
            }
            | Self::Status {
                protocol_version, ..
            }
            | Self::Error {
                protocol_version, ..
            } => *protocol_version,
        }
    }
}

/// Client for the local queue-ingress privilege boundary.
#[derive(Clone, Debug)]
pub struct IngressClient {
    socket_path: std::path::PathBuf,
}

impl IngressClient {
    #[must_use]
    pub fn new(socket_path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
        }
    }

    /// Forwards one authenticated package stream to the queue owner.
    pub fn submit(
        &self,
        client_id: ClientId,
        mut input: impl Read,
        timeout: Duration,
    ) -> Result<SubmitResponse, IngressClientError> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(IngressClientError::DeadlineExceeded)?;
        let request = IngressRequest::Submit {
            protocol_version: CURRENT_INGRESS_PROTOCOL_VERSION,
            client_id,
        };
        let mut stream = self.connect(deadline)?;
        let send_result = (|| {
            let mut output = DeadlineWriter::new(&mut stream, deadline);
            write_header(&mut output, &request)?;
            io::copy(&mut input, &mut output)
                .map(|_| ())
                .map_err(IngressClientError::Transport)
        })()
        .and_then(|()| {
            stream
                .shutdown(std::net::Shutdown::Write)
                .map_err(IngressClientError::Transport)
        });
        if let Err(send_error) = send_result {
            let _ = stream.shutdown(std::net::Shutdown::Write);
            if let Ok(IngressResponse::Error { error, .. }) =
                read_response_until(&mut stream, deadline)
            {
                return Err(IngressClientError::Broker(error.error_code));
            }
            return Err(send_error);
        }
        match read_response_until(&mut stream, deadline)? {
            IngressResponse::Submit { response, .. } => Ok(response),
            IngressResponse::Error { error, .. } => {
                Err(IngressClientError::Broker(error.error_code))
            }
            IngressResponse::Status { .. } => Err(IngressClientError::UnexpectedResponse),
        }
    }

    /// Requests one durable queue state from the queue owner.
    pub fn status(
        &self,
        request: StatusRequest,
        timeout: Duration,
    ) -> Result<StatusResponse, IngressClientError> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(IngressClientError::DeadlineExceeded)?;
        let expected_request_id = request.request_id;
        let request = IngressRequest::Status {
            protocol_version: CURRENT_INGRESS_PROTOCOL_VERSION,
            request_id: expected_request_id,
        };
        let mut stream = self.connect(deadline)?;
        {
            let mut output = DeadlineWriter::new(&mut stream, deadline);
            write_header(&mut output, &request)?;
        }
        stream
            .shutdown(std::net::Shutdown::Write)
            .map_err(IngressClientError::Transport)?;
        match read_response_until(&mut stream, deadline)? {
            IngressResponse::Status { response, .. }
                if response.request_id() == expected_request_id =>
            {
                Ok(response)
            }
            IngressResponse::Status { .. } => Err(IngressClientError::UnexpectedResponse),
            IngressResponse::Error { error, .. } => {
                Err(IngressClientError::Broker(error.error_code))
            }
            IngressResponse::Submit { .. } => Err(IngressClientError::UnexpectedResponse),
        }
    }

    fn connect(&self, deadline: Instant) -> Result<UnixStream, IngressClientError> {
        let stream = connect_until(&self.socket_path, deadline)?;
        set_remaining_timeout(&stream, deadline)?;
        Ok(stream)
    }
}

#[cfg(target_os = "linux")]
fn connect_until(path: &Path, deadline: Instant) -> Result<UnixStream, IngressClientError> {
    use std::os::fd::{AsFd, AsRawFd};

    use nix::errno::Errno;
    use nix::fcntl::{FcntlArg, OFlag, fcntl};
    use nix::poll::{PollFd, PollFlags, PollTimeout, poll};
    use nix::sys::socket::{
        AddressFamily, SockFlag, SockType, UnixAddr, connect, getsockopt, socket,
        sockopt::SocketError,
    };

    let address = UnixAddr::new(path).map_err(nix_transport_error)?;
    let descriptor = socket(
        AddressFamily::Unix,
        SockType::Stream,
        SockFlag::SOCK_CLOEXEC | SockFlag::SOCK_NONBLOCK,
        None,
    )
    .map_err(nix_transport_error)?;
    match connect(descriptor.as_raw_fd(), &address) {
        Ok(()) => {}
        Err(Errno::EINPROGRESS) => {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .filter(|remaining| !remaining.is_zero())
                .ok_or(IngressClientError::DeadlineExceeded)?;
            let timeout = PollTimeout::try_from(remaining).unwrap_or(PollTimeout::MAX);
            let mut descriptors = [PollFd::new(descriptor.as_fd(), PollFlags::POLLOUT)];
            if poll(&mut descriptors, timeout).map_err(nix_transport_error)? == 0 {
                return Err(IngressClientError::DeadlineExceeded);
            }
            let pending = getsockopt(&descriptor, SocketError).map_err(nix_transport_error)?;
            if pending != 0 {
                return Err(IngressClientError::Transport(io::Error::from_raw_os_error(
                    pending,
                )));
            }
        }
        Err(error) => return Err(nix_transport_error(error)),
    }
    fcntl(&descriptor, FcntlArg::F_SETFL(OFlag::empty())).map_err(nix_transport_error)?;
    Ok(UnixStream::from(descriptor))
}

#[cfg(target_os = "linux")]
fn nix_transport_error(error: nix::errno::Errno) -> IngressClientError {
    IngressClientError::Transport(io::Error::from_raw_os_error(error as i32))
}

#[cfg(not(target_os = "linux"))]
fn connect_until(path: &Path, deadline: Instant) -> Result<UnixStream, IngressClientError> {
    if Instant::now() >= deadline {
        return Err(IngressClientError::DeadlineExceeded);
    }
    UnixStream::connect(path).map_err(IngressClientError::Transport)
}

fn set_remaining_timeout(stream: &UnixStream, deadline: Instant) -> Result<(), IngressClientError> {
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or(IngressClientError::DeadlineExceeded)?;
    stream
        .set_read_timeout(Some(remaining))
        .and_then(|()| stream.set_write_timeout(Some(remaining)))
        .map_err(IngressClientError::Transport)
}

struct DeadlineReader<'a> {
    stream: &'a mut UnixStream,
    deadline: Instant,
}

impl<'a> DeadlineReader<'a> {
    const fn new(stream: &'a mut UnixStream, deadline: Instant) -> Self {
        Self { stream, deadline }
    }
}

impl Read for DeadlineReader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        loop {
            let remaining = remaining_until(self.deadline)?;
            self.stream.set_read_timeout(Some(remaining))?;
            match self.stream.read(buffer) {
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::Interrupted
                            | io::ErrorKind::WouldBlock
                            | io::ErrorKind::TimedOut
                    ) =>
                {
                    if Instant::now() >= self.deadline {
                        return Err(deadline_io_error());
                    }
                }
                result => return result,
            }
        }
    }
}

struct DeadlineWriter<'a> {
    stream: &'a mut UnixStream,
    deadline: Instant,
}

impl<'a> DeadlineWriter<'a> {
    const fn new(stream: &'a mut UnixStream, deadline: Instant) -> Self {
        Self { stream, deadline }
    }
}

impl Write for DeadlineWriter<'_> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        loop {
            let remaining = remaining_until(self.deadline)?;
            self.stream.set_write_timeout(Some(remaining))?;
            match self.stream.write(buffer) {
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::Interrupted
                            | io::ErrorKind::WouldBlock
                            | io::ErrorKind::TimedOut
                    ) =>
                {
                    if Instant::now() >= self.deadline {
                        return Err(deadline_io_error());
                    }
                }
                result => return result,
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn remaining_until(deadline: Instant) -> io::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(deadline_io_error)
}

fn deadline_io_error() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "queue ingress deadline expired")
}

fn write_header(
    output: &mut impl Write,
    request: &IngressRequest,
) -> Result<(), IngressClientError> {
    let header = serde_json::to_vec(request).map_err(IngressClientError::Json)?;
    if header.len().saturating_add(1) as u64 > MAXIMUM_HEADER_BYTES {
        return Err(IngressClientError::RequestHeaderTooLarge);
    }
    output
        .write_all(&header)
        .and_then(|()| output.write_all(b"\n"))
        .map_err(IngressClientError::Transport)
}

fn read_response_until(
    stream: &mut UnixStream,
    deadline: Instant,
) -> Result<IngressResponse, IngressClientError> {
    set_remaining_timeout(stream, deadline)?;
    let mut input = DeadlineReader::new(stream, deadline);
    read_response(&mut input)
}

fn read_response(input: &mut impl Read) -> Result<IngressResponse, IngressClientError> {
    let mut input = BufReader::new(input);
    let bytes = read_bounded_line(&mut input, MAXIMUM_RESPONSE_BYTES)
        .map_err(IngressClientError::Transport)?;
    let mut trailing = [0_u8; 1];
    if input
        .read(&mut trailing)
        .map_err(IngressClientError::Transport)?
        != 0
    {
        return Err(IngressClientError::UnexpectedResponse);
    }
    let response: IngressResponse =
        serde_json::from_slice(&bytes).map_err(IngressClientError::Json)?;
    if response.protocol_version() != CURRENT_INGRESS_PROTOCOL_VERSION {
        return Err(IngressClientError::UnsupportedVersion);
    }
    Ok(response)
}

/// Serves one systemd-activated queue-ingress connection.
///
/// A protocol error is written before the underlying error is returned for
/// service-manager diagnostics.
pub fn serve(
    queue_root: &Path,
    input: impl Read,
    mut output: impl Write,
) -> Result<(), IngressServeError> {
    let result = serve_request(queue_root, input);
    let response = match &result {
        Ok(response) => (*response).clone(),
        Err(error) => IngressResponse::error(error.error_code()),
    };
    serde_json::to_writer(&mut output, &response).map_err(IngressServeError::ResponseJson)?;
    output
        .write_all(b"\n")
        .map_err(IngressServeError::ResponseIo)?;
    result.map(|_| ())
}

fn serve_request(
    queue_root: &Path,
    input: impl Read,
) -> Result<IngressResponse, IngressServeError> {
    let mut input = BufReader::new(input);
    let header = read_bounded_line(&mut input, MAXIMUM_HEADER_BYTES)
        .map_err(IngressServeError::RequestIo)?;
    let request: IngressRequest =
        serde_json::from_slice(&header).map_err(IngressServeError::RequestJson)?;
    if request.protocol_version() != CURRENT_INGRESS_PROTOCOL_VERSION {
        return Err(IngressServeError::UnsupportedVersion);
    }
    match request {
        IngressRequest::Submit { client_id, .. } => {
            let queue = open_queue(queue_root)?;
            submit::submit(&queue, client_id, &mut input)
                .map(IngressResponse::submit)
                .map_err(IngressServeError::Gateway)
        }
        IngressRequest::Status { request_id, .. } => {
            ensure_end(&mut input)?;
            status_response(queue_root, request_id).map(IngressResponse::status)
        }
    }
}

fn open_queue(queue_root: &Path) -> Result<FileQueue, IngressServeError> {
    let resolved =
        PathAttestation::resolve_destination(queue_root).map_err(IngressServeError::Attestation)?;
    let queue = FileQueue::initialize(resolved.stable_path(), PackagePolicy::default())
        .map_err(|error| IngressServeError::Gateway(GatewayError::Queue(Box::new(error))))?;
    let observed = queue
        .storage_attestation()
        .map_err(IngressServeError::Attestation)?;
    if !resolved.matches_destination(&observed) {
        return Err(IngressServeError::Attestation(
            PathAttestationError::BindingMismatch,
        ));
    }
    Ok(queue)
}

fn status_response(
    queue_root: &Path,
    request_id: RequestId,
) -> Result<StatusResponse, IngressServeError> {
    let resolved =
        PathAttestation::resolve_destination(queue_root).map_err(IngressServeError::Attestation)?;
    let queue = QueueReader::open_until(resolved.stable_path(), None)
        .map_err(|error| IngressServeError::Gateway(GatewayError::Queue(Box::new(error))))?;
    let observed = queue
        .storage_attestation()
        .map_err(IngressServeError::Attestation)?;
    if !resolved.matches_destination(&observed) {
        return Err(IngressServeError::Attestation(
            PathAttestationError::BindingMismatch,
        ));
    }
    let status = queue
        .status_until(request_id, None)
        .map_err(|error| IngressServeError::Gateway(GatewayError::Queue(Box::new(error))))?
        .ok_or(IngressServeError::Gateway(GatewayError::RequestNotFound {
            request_id,
        }))?;
    let status = match status {
        QueueRequestStatus::Pending => RequestStatus::Pending,
        QueueRequestStatus::Processing => RequestStatus::Processing,
        QueueRequestStatus::Completed => RequestStatus::Completed,
        QueueRequestStatus::Failed {
            error_code,
            failed_at,
        } => RequestStatus::Failed {
            error_code,
            failed_at,
        },
    };
    Ok(StatusResponse::new(request_id, status))
}

fn ensure_end(input: &mut impl Read) -> Result<(), IngressServeError> {
    let mut trailing = [0_u8; 1];
    if input
        .read(&mut trailing)
        .map_err(IngressServeError::RequestIo)?
        == 0
    {
        Ok(())
    } else {
        Err(IngressServeError::TrailingData)
    }
}

fn read_bounded_line(input: &mut impl BufRead, maximum: u64) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let count = input
        .take(maximum.saturating_add(1))
        .read_until(b'\n', &mut bytes)?;
    if count == 0 || count as u64 > maximum || bytes.last() != Some(&b'\n') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bounded newline-delimited message is incomplete or oversized",
        ));
    }
    bytes.pop();
    if bytes.contains(&b'\n') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "message contains an unexpected newline",
        ));
    }
    Ok(bytes)
}

/// Failure while using the local queue-ingress socket.
#[derive(Debug)]
pub enum IngressClientError {
    Transport(io::Error),
    Json(serde_json::Error),
    DeadlineExceeded,
    RequestHeaderTooLarge,
    UnsupportedVersion,
    UnexpectedResponse,
    Broker(ErrorCode),
}

impl IngressClientError {
    #[must_use]
    pub const fn error_code(&self) -> ErrorCode {
        match self {
            Self::Broker(code) => *code,
            Self::DeadlineExceeded | Self::Transport(_) => ErrorCode::TemporaryFailure,
            Self::Json(_)
            | Self::RequestHeaderTooLarge
            | Self::UnsupportedVersion
            | Self::UnexpectedResponse => ErrorCode::InternalError,
        }
    }
}

impl fmt::Display for IngressClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(error) => write!(formatter, "queue ingress transport failed: {error}"),
            Self::Json(error) => write!(formatter, "queue ingress JSON failed: {error}"),
            Self::DeadlineExceeded => formatter.write_str("queue ingress deadline expired"),
            Self::RequestHeaderTooLarge => {
                formatter.write_str("queue ingress request header exceeded its internal bound")
            }
            Self::UnsupportedVersion => {
                formatter.write_str("queue ingress returned an unsupported protocol version")
            }
            Self::UnexpectedResponse => {
                formatter.write_str("queue ingress returned the wrong response kind")
            }
            Self::Broker(code) => write!(
                formatter,
                "queue ingress rejected the operation with {code}"
            ),
        }
    }
}

impl std::error::Error for IngressClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            Self::Json(error) => Some(error),
            _ => None,
        }
    }
}

/// Failure while serving one local queue-ingress connection.
#[derive(Debug)]
pub enum IngressServeError {
    RequestIo(io::Error),
    RequestJson(serde_json::Error),
    UnsupportedVersion,
    TrailingData,
    Attestation(PathAttestationError),
    Gateway(GatewayError),
    ResponseJson(serde_json::Error),
    ResponseIo(io::Error),
}

impl IngressServeError {
    #[must_use]
    pub fn error_code(&self) -> ErrorCode {
        match self {
            Self::RequestIo(error) if error.kind() == io::ErrorKind::InvalidData => {
                ErrorCode::InvalidProtocol
            }
            Self::RequestJson(_) | Self::UnsupportedVersion | Self::TrailingData => {
                ErrorCode::InvalidProtocol
            }
            Self::Gateway(error) => error.error_code(),
            Self::RequestIo(_)
            | Self::Attestation(_)
            | Self::ResponseJson(_)
            | Self::ResponseIo(_) => ErrorCode::InternalError,
        }
    }
}

impl fmt::Display for IngressServeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RequestIo(error) => {
                write!(formatter, "queue ingress request read failed: {error}")
            }
            Self::RequestJson(error) => write!(formatter, "invalid queue ingress request: {error}"),
            Self::UnsupportedVersion => {
                formatter.write_str("unsupported queue ingress protocol version")
            }
            Self::TrailingData => {
                formatter.write_str("queue ingress status request has trailing data")
            }
            Self::Attestation(error) => write!(
                formatter,
                "queue ingress storage attestation failed: {error}"
            ),
            Self::Gateway(error) => error.fmt(formatter),
            Self::ResponseJson(error) => {
                write!(formatter, "queue ingress response encoding failed: {error}")
            }
            Self::ResponseIo(error) => {
                write!(formatter, "queue ingress response write failed: {error}")
            }
        }
    }
}

impl std::error::Error for IngressServeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::RequestIo(error) | Self::ResponseIo(error) => Some(error),
            Self::RequestJson(error) | Self::ResponseJson(error) => Some(error),
            Self::Attestation(error) => Some(error),
            Self::Gateway(error) => Some(error),
            Self::UnsupportedVersion | Self::TrailingData => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::{Cursor, Read, Write};
    use std::os::unix::net::UnixListener;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    use agent_knowledge_core::ErrorCode;
    use agent_knowledge_protocol::{
        ClientId, RequestStatus, StatusRequest, StatusResponse, SubmitOutcome,
    };
    use tar::{Builder, EntryType, Header};

    use super::{IngressClient, IngressClientError, IngressServeError, serve};

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn create() -> Self {
            let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "agent-knowledge-ingress-test-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir(&path)
                .unwrap_or_else(|error| panic!("ingress test directory must be created: {error}"));
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            if let Err(error) = fs::remove_dir_all(&self.0)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                panic!("ingress test directory must be removed: {error}");
            }
        }
    }

    fn archive() -> Vec<u8> {
        const REQUEST: &[u8] = br#"{
  "protocol_version": 1,
  "request_id": "01K00000000000000000000000",
  "title": "Record a fictional ingress test",
  "project": "fictional-project",
  "document_type": "experiment",
  "created_at": "2026-07-31T03:50:00Z",
  "operations": [{
    "type": "create_document",
    "document_id": "01K00000000000000000000001",
    "content": "run/index.md"
  }]
}"#;
        const MARKDOWN: &[u8] = b"---\n\
schema_version: 1\n\
document_id: 01K00000000000000000000001\n\
title: Fictional ingress test\n\
created: 2026-07-31T03:50:00Z\n\
request_id: 01K00000000000000000000000\n\
status: active\n\
---\n\
Fictional ingress body.\n";
        let mut builder = Builder::new(Vec::new());
        append(&mut builder, "request.json", REQUEST);
        append(&mut builder, "payload/run/index.md", MARKDOWN);
        builder
            .into_inner()
            .unwrap_or_else(|error| panic!("ingress tar fixture must finish: {error}"))
    }

    fn early_invalid_archive() -> Vec<u8> {
        let mut builder = Builder::new(Vec::new());
        let mut header = Header::new_gnu();
        header.set_entry_type(EntryType::Symlink);
        header.set_mode(0o644);
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(0);
        header.set_size(0);
        header
            .set_path("invalid-link")
            .unwrap_or_else(|error| panic!("invalid archive path fixture must encode: {error}"));
        header
            .set_link_name("fictional-target")
            .unwrap_or_else(|error| panic!("invalid archive link fixture must encode: {error}"));
        header.set_cksum();
        builder
            .append(&header, Cursor::new(Vec::<u8>::new()))
            .unwrap_or_else(|error| panic!("invalid archive fixture must append: {error}"));
        let mut archive = builder
            .into_inner()
            .unwrap_or_else(|error| panic!("invalid archive fixture must finish: {error}"));
        archive.resize(8 * 1024 * 1024, 0x5a);
        archive
    }

    fn append(builder: &mut Builder<Vec<u8>>, path: &str, bytes: &[u8]) {
        let mut header = Header::new_gnu();
        header.set_entry_type(EntryType::Regular);
        header.set_mode(0o644);
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(0);
        header.set_size(bytes.len() as u64);
        header.set_cksum();
        builder
            .append_data(&mut header, path, Cursor::new(bytes))
            .unwrap_or_else(|error| panic!("ingress tar entry must append: {error}"));
    }

    #[test]
    fn submit_and_status_cross_only_the_unix_socket() {
        let root = TestDirectory::create();
        let queue = root.path().join("queue");
        let socket = root.path().join("ingress.sock");
        let listener = UnixListener::bind(&socket)
            .unwrap_or_else(|error| panic!("ingress socket fixture must bind: {error}"));
        let queue_for_server = queue.clone();
        let server = std::thread::spawn(move || {
            for _ in 0..2 {
                let (stream, _) = listener
                    .accept()
                    .unwrap_or_else(|error| panic!("ingress fixture must accept: {error}"));
                let input = stream
                    .try_clone()
                    .unwrap_or_else(|error| panic!("ingress stream must clone: {error}"));
                serve(&queue_for_server, input, stream)
                    .unwrap_or_else(|error| panic!("ingress request must succeed: {error}"));
            }
        });

        let client = IngressClient::new(&socket);
        let client_id: ClientId = "fictional-node-a"
            .parse()
            .unwrap_or_else(|error| panic!("client fixture must parse: {error}"));
        let submitted = client
            .submit(client_id, Cursor::new(archive()), Duration::from_secs(5))
            .unwrap_or_else(|error| panic!("ingress submit must succeed: {error}"));
        assert!(matches!(submitted.outcome, SubmitOutcome::Accepted { .. }));

        let request_id = "01K00000000000000000000000"
            .parse()
            .unwrap_or_else(|error| panic!("request fixture must parse: {error}"));
        let status = client
            .status(StatusRequest::new(request_id), Duration::from_secs(5))
            .unwrap_or_else(|error| panic!("ingress status must succeed: {error}"));
        assert_eq!(status.request_status(), RequestStatus::Pending);
        server
            .join()
            .unwrap_or_else(|_| panic!("ingress fixture server must join"));
    }

    #[test]
    fn strict_broker_errors_are_machine_readable() {
        let root = TestDirectory::create();
        let mut output = Vec::new();
        let error = match serve(
            &root.path().join("queue"),
            Cursor::new(b"{\"protocol_version\":1,\"operation\":\"status\",\"request_id\":\"01K00000000000000000000000\",\"extra\":true}\n"),
            &mut output,
        ) {
            Ok(()) => panic!("unknown ingress request fields must fail"),
            Err(error) => error,
        };
        assert!(matches!(error, IngressServeError::RequestJson(_)));
        let response: serde_json::Value = serde_json::from_slice(&output)
            .unwrap_or_else(|error| panic!("broker error response must decode: {error}"));
        assert_eq!(response["error"]["error_code"], "INVALID_PROTOCOL");
    }

    #[test]
    fn broker_error_codes_cross_the_client_boundary() {
        let root = TestDirectory::create();
        let queue = root.path().join("queue");
        super::open_queue(&queue)
            .unwrap_or_else(|error| panic!("queue fixture must initialize: {error}"));
        let socket = root.path().join("ingress.sock");
        let listener = UnixListener::bind(&socket)
            .unwrap_or_else(|error| panic!("ingress socket fixture must bind: {error}"));
        let server = std::thread::spawn(move || {
            let (stream, _) = listener
                .accept()
                .unwrap_or_else(|error| panic!("ingress fixture must accept: {error}"));
            let input = stream
                .try_clone()
                .unwrap_or_else(|error| panic!("ingress stream must clone: {error}"));
            assert!(serve(&queue, input, stream).is_err());
        });
        let request_id = "01K00000000000000000000099"
            .parse()
            .unwrap_or_else(|error| panic!("missing request fixture must parse: {error}"));
        let error = match IngressClient::new(&socket)
            .status(StatusRequest::new(request_id), Duration::from_secs(5))
        {
            Ok(_) => panic!("missing request status must fail"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            IngressClientError::Broker(ErrorCode::RequestNotFound)
        ));
        server
            .join()
            .unwrap_or_else(|_| panic!("ingress fixture server must join"));
    }

    #[test]
    fn early_broker_rejection_takes_precedence_over_a_broken_submit_write() {
        let root = TestDirectory::create();
        let queue = root.path().join("queue");
        let socket = root.path().join("ingress.sock");
        let listener = UnixListener::bind(&socket)
            .unwrap_or_else(|error| panic!("ingress socket fixture must bind: {error}"));
        let server = std::thread::spawn(move || {
            let (stream, _) = listener
                .accept()
                .unwrap_or_else(|error| panic!("ingress fixture must accept: {error}"));
            let input = stream
                .try_clone()
                .unwrap_or_else(|error| panic!("ingress stream must clone: {error}"));
            assert!(serve(&queue, input, stream).is_err());
        });
        let client_id: ClientId = "fictional-node-a"
            .parse()
            .unwrap_or_else(|error| panic!("client fixture must parse: {error}"));
        let error = match IngressClient::new(&socket).submit(
            client_id,
            Cursor::new(early_invalid_archive()),
            Duration::from_secs(5),
        ) {
            Ok(_) => panic!("early invalid archive must be rejected"),
            Err(error) => error,
        };
        assert!(
            matches!(error, IngressClientError::Broker(ErrorCode::InvalidPath)),
            "unexpected ingress error: {error:?}"
        );
        server
            .join()
            .unwrap_or_else(|_| panic!("ingress fixture server must join"));
    }

    #[test]
    fn request_header_socket_failures_are_transport_errors() {
        struct BrokenWriter;

        impl Write for BrokenWriter {
            fn write(&mut self, _buffer: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "fictional disconnected broker",
                ))
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let request_id = "01K00000000000000000000000"
            .parse()
            .unwrap_or_else(|error| panic!("request fixture must parse: {error}"));
        let error = match super::write_header(
            &mut BrokenWriter,
            &super::IngressRequest::Status {
                protocol_version: super::CURRENT_INGRESS_PROTOCOL_VERSION,
                request_id,
            },
        ) {
            Ok(()) => panic!("disconnected header writer must fail"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            IngressClientError::Transport(error)
                if error.kind() == std::io::ErrorKind::BrokenPipe
        ));
    }

    #[test]
    fn submit_writes_stop_at_one_absolute_deadline() {
        let root = TestDirectory::create();
        let socket = root.path().join("ingress.sock");
        let listener = UnixListener::bind(&socket)
            .unwrap_or_else(|error| panic!("ingress socket fixture must bind: {error}"));
        let server = std::thread::spawn(move || {
            let (_stream, _) = listener
                .accept()
                .unwrap_or_else(|error| panic!("ingress fixture must accept: {error}"));
            std::thread::sleep(Duration::from_millis(500));
        });

        let client_id: ClientId = "fictional-node-a"
            .parse()
            .unwrap_or_else(|error| panic!("client fixture must parse: {error}"));
        let started = Instant::now();
        let result = IngressClient::new(&socket).submit(
            client_id,
            Cursor::new(vec![0_u8; 8 * 1024 * 1024]),
            Duration::from_millis(25),
        );
        assert!(matches!(
            result,
            Err(IngressClientError::Transport(error))
                if error.kind() == std::io::ErrorKind::TimedOut
        ));
        assert!(started.elapsed() < Duration::from_millis(500));
        server
            .join()
            .unwrap_or_else(|_| panic!("ingress fixture server must join"));
    }

    #[test]
    fn response_reads_stop_at_one_absolute_deadline() {
        let root = TestDirectory::create();
        let socket = root.path().join("ingress.sock");
        let listener = UnixListener::bind(&socket)
            .unwrap_or_else(|error| panic!("ingress socket fixture must bind: {error}"));
        let request_id = "01K00000000000000000000000"
            .parse()
            .unwrap_or_else(|error| panic!("request fixture must parse: {error}"));
        let response = serde_json::to_vec(&super::IngressResponse::status(StatusResponse::new(
            request_id,
            RequestStatus::Pending,
        )))
        .unwrap_or_else(|error| panic!("ingress response fixture must encode: {error}"));
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener
                .accept()
                .unwrap_or_else(|error| panic!("ingress fixture must accept: {error}"));
            let mut request = Vec::new();
            stream
                .read_to_end(&mut request)
                .unwrap_or_else(|error| panic!("ingress fixture must read request: {error}"));
            for byte in response {
                if stream.write_all(&[byte]).is_err() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        });

        let started = Instant::now();
        let result = IngressClient::new(&socket)
            .status(StatusRequest::new(request_id), Duration::from_millis(25));
        assert!(matches!(
            result,
            Err(IngressClientError::Transport(error))
                if error.kind() == std::io::ErrorKind::TimedOut
        ));
        assert!(started.elapsed() < Duration::from_millis(200));
        server
            .join()
            .unwrap_or_else(|_| panic!("ingress fixture server must join"));
    }
}

use std::ffi::{OsStr, OsString};
use std::fmt;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::thread;
use std::time::{Duration, Instant};

use agent_knowledge_core::{
    AttachmentName, DocumentId, PinnedDirectory, PinnedPathError, RequestId, Revision,
    decode_document_metadata,
};
use agent_knowledge_protocol::{
    CURRENT_GATEWAY_PROTOCOL_VERSION, EXPORT_COMMAND, ExportRequest, GET_COMMAND, GetRequest,
    GetResponse, LIST_COMMAND, ListRequest, ListResponse, ProtocolErrorResponse, RECENT_COMMAND,
    SEARCH_COMMAND, STATUS_COMMAND, SUBMIT_COMMAND, SearchRequest, StatusRequest, StatusResponse,
    SubmitOutcome, SubmitResponse,
};
use agent_knowledge_queue::{
    PackagePolicy, PackageValidationError, PayloadMetadata, ValidatedPackage, validate_package,
};
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use tar::{Builder, EntryType, Header};

pub mod cli;

const SSH_PROGRAM: &str = "ssh";
const MAXIMUM_RESPONSE_BYTES: u64 = 64 * 1024;
const MAXIMUM_CONTROL_REQUEST_BYTES: u64 = 64 * 1024;
const MAXIMUM_CONTROL_RESPONSE_BYTES: u64 = 256 * 1024 * 1024;
const MAXIMUM_STATUS_RESPONSE_BYTES: u64 = 4 * 1024;

/// Typed SSH transport for Agent Knowledge Gateway operations.
#[derive(Clone, Debug)]
pub struct SshClient {
    destination: OsString,
    timeout: Duration,
}

impl SshClient {
    /// Creates a client using the local OpenSSH configuration and credentials.
    ///
    /// # Errors
    ///
    /// Returns an error when the destination is empty or the timeout is zero.
    pub fn new(
        destination: impl Into<OsString>,
        timeout: Duration,
    ) -> Result<Self, ClientCommandError> {
        let destination = destination.into();
        validate_connection(&destination, timeout)?;
        Ok(Self {
            destination,
            timeout,
        })
    }

    /// Submits one validated request package.
    ///
    /// # Errors
    ///
    /// Returns an error when the package is invalid or the SSH submission fails.
    pub fn submit(&self, package_root: &Path) -> Result<SubmitResponse, ClientCommandError> {
        submit_response_with_program(
            OsStr::new(SSH_PROGRAM),
            &self.destination,
            package_root,
            self.timeout,
        )
        .map(|(response, _diagnostic)| response)
    }

    /// Lists committed documents in canonical path order.
    ///
    /// # Errors
    ///
    /// Returns an error when the Gateway request fails or its response is invalid.
    pub fn list(&self, request: &ListRequest) -> Result<ListResponse, ClientCommandError> {
        self.read_list(LIST_COMMAND, request)
    }

    /// Lists recently committed documents.
    ///
    /// # Errors
    ///
    /// Returns an error when the Gateway request fails or its response is invalid.
    pub fn recent(&self, request: &ListRequest) -> Result<ListResponse, ClientCommandError> {
        self.read_list(RECENT_COMMAND, request)
    }

    /// Searches committed Markdown and permitted metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when the Gateway request fails or its response is invalid.
    pub fn search(&self, request: &SearchRequest) -> Result<ListResponse, ClientCommandError> {
        control_response_with_program::<_, ListResponse>(
            OsStr::new(SSH_PROGRAM),
            &self.destination,
            ControlOperation::new(SEARCH_COMMAND, MAXIMUM_CONTROL_RESPONSE_BYTES),
            request,
            self.timeout,
        )
        .map(|(response, _diagnostic)| response)
    }

    /// Gets one committed Markdown document by permanent identifier.
    ///
    /// # Errors
    ///
    /// Returns an error when the Gateway request fails or identifies another document.
    pub fn get(&self, request: &GetRequest) -> Result<GetResponse, ClientCommandError> {
        get_response_with_program(
            OsStr::new(SSH_PROGRAM),
            &self.destination,
            request,
            self.timeout,
        )
        .map(|(response, _diagnostic)| response)
    }

    /// Gets the durable state of one accepted request.
    ///
    /// # Errors
    ///
    /// Returns an error when the Gateway request fails or identifies another request.
    pub fn status(&self, request: &StatusRequest) -> Result<StatusResponse, ClientCommandError> {
        status_response_with_program(
            OsStr::new(SSH_PROGRAM),
            &self.destination,
            request,
            self.timeout,
        )
        .map(|(response, _diagnostic)| response)
    }

    fn read_list(
        &self,
        remote_command: &str,
        request: &ListRequest,
    ) -> Result<ListResponse, ClientCommandError> {
        control_response_with_program::<_, ListResponse>(
            OsStr::new(SSH_PROGRAM),
            &self.destination,
            ControlOperation::new(remote_command, MAXIMUM_CONTROL_RESPONSE_BYTES),
            request,
            self.timeout,
        )
        .map(|(response, _diagnostic)| response)
    }
}

#[derive(Clone, Copy)]
struct ControlOperation<'a> {
    remote_command: &'a str,
    maximum_response_bytes: u64,
}

impl<'a> ControlOperation<'a> {
    const fn new(remote_command: &'a str, maximum_response_bytes: u64) -> Self {
        Self {
            remote_command,
            maximum_response_bytes,
        }
    }
}

pub(crate) fn submit(
    destination: &OsStr,
    package_root: &Path,
    timeout: Duration,
    output: impl Write,
) -> Result<(), ClientCommandError> {
    submit_with_program(
        OsStr::new(SSH_PROGRAM),
        destination,
        package_root,
        timeout,
        output,
        io::stderr(),
    )
}

pub(crate) fn control<Request, Response>(
    destination: &OsStr,
    remote_command: &str,
    request: &Request,
    timeout: Duration,
    output: impl Write,
) -> Result<(), ClientCommandError>
where
    Request: Serialize,
    Response: DeserializeOwned + Serialize,
{
    control_with_program::<Request, Response>(
        OsStr::new(SSH_PROGRAM),
        destination,
        ControlOperation::new(remote_command, MAXIMUM_CONTROL_RESPONSE_BYTES),
        request,
        timeout,
        output,
        io::stderr(),
    )
}

pub(crate) fn get(
    destination: &OsStr,
    request: &GetRequest,
    timeout: Duration,
    output: impl Write,
) -> Result<(), ClientCommandError> {
    get_with_program(
        OsStr::new(SSH_PROGRAM),
        destination,
        request,
        timeout,
        output,
        io::stderr(),
    )
}

pub(crate) fn export(
    destination: &OsStr,
    request: &ExportRequest,
    timeout: Duration,
    output: impl Write,
) -> Result<(), ClientCommandError> {
    export_with_program(
        OsStr::new(SSH_PROGRAM),
        destination,
        request,
        timeout,
        output,
        io::stderr(),
    )
}

pub(crate) fn status(
    destination: &OsStr,
    request: &StatusRequest,
    timeout: Duration,
    output: impl Write,
) -> Result<(), ClientCommandError> {
    status_with_program(
        OsStr::new(SSH_PROGRAM),
        destination,
        request,
        timeout,
        output,
        io::stderr(),
    )
}

fn status_with_program(
    program: &OsStr,
    destination: &OsStr,
    request: &StatusRequest,
    timeout: Duration,
    mut output: impl Write,
    mut diagnostic_output: impl Write,
) -> Result<(), ClientCommandError> {
    let (response, diagnostic) =
        status_response_with_program(program, destination, request, timeout)?;
    diagnostic_output
        .write_all(&diagnostic)
        .map_err(ClientCommandError::DiagnosticOutput)?;
    write_json_response(&mut output, &response)
}

fn status_response_with_program(
    program: &OsStr,
    destination: &OsStr,
    request: &StatusRequest,
    timeout: Duration,
) -> Result<(StatusResponse, Vec<u8>), ClientCommandError> {
    let (response, diagnostic) = control_response_with_program::<_, StatusResponse>(
        program,
        destination,
        ControlOperation::new(STATUS_COMMAND, MAXIMUM_STATUS_RESPONSE_BYTES),
        request,
        timeout,
    )?;
    if response.request_id() != request.request_id {
        return Err(ClientCommandError::RequestStatusResponseMismatch);
    }
    Ok((response, diagnostic))
}

fn get_with_program(
    program: &OsStr,
    destination: &OsStr,
    request: &GetRequest,
    timeout: Duration,
    mut output: impl Write,
    mut diagnostic_output: impl Write,
) -> Result<(), ClientCommandError> {
    let (response, diagnostic) = get_response_with_program(program, destination, request, timeout)?;
    diagnostic_output
        .write_all(&diagnostic)
        .map_err(ClientCommandError::DiagnosticOutput)?;
    write_json_response(&mut output, &response)
}

fn get_response_with_program(
    program: &OsStr,
    destination: &OsStr,
    request: &GetRequest,
    timeout: Duration,
) -> Result<(GetResponse, Vec<u8>), ClientCommandError> {
    let (response, diagnostic) = control_response_with_program::<_, GetResponse>(
        program,
        destination,
        ControlOperation::new(GET_COMMAND, MAXIMUM_CONTROL_RESPONSE_BYTES),
        request,
        timeout,
    )?;
    if response.document.summary.metadata.document_id != request.document_id {
        return Err(ClientCommandError::DocumentResponseMismatch);
    }
    Ok((response, diagnostic))
}

fn control_with_program<Request, Response>(
    program: &OsStr,
    destination: &OsStr,
    operation: ControlOperation<'_>,
    request: &Request,
    timeout: Duration,
    mut output: impl Write,
    mut diagnostic_output: impl Write,
) -> Result<(), ClientCommandError>
where
    Request: Serialize,
    Response: DeserializeOwned + Serialize,
{
    let (response, diagnostic) = control_response_with_program::<Request, Response>(
        program,
        destination,
        operation,
        request,
        timeout,
    )?;
    diagnostic_output
        .write_all(&diagnostic)
        .map_err(ClientCommandError::DiagnosticOutput)?;
    write_json_response(&mut output, &response)
}

fn control_response_with_program<Request, Response>(
    program: &OsStr,
    destination: &OsStr,
    operation: ControlOperation<'_>,
    request: &Request,
    timeout: Duration,
) -> Result<(Response, Vec<u8>), ClientCommandError>
where
    Request: Serialize,
    Response: DeserializeOwned,
{
    let (response, diagnostic) =
        execute_control_with_program(program, destination, operation, request, timeout)?;
    let protocol_version =
        decode_protocol_version(&response).map_err(ClientCommandError::InvalidResponse)?;
    require_current_protocol(protocol_version)?;
    let response =
        serde_json::from_slice(&response).map_err(ClientCommandError::InvalidResponse)?;
    Ok((response, diagnostic))
}

fn execute_control_with_program<Request>(
    program: &OsStr,
    destination: &OsStr,
    operation: ControlOperation<'_>,
    request: &Request,
    timeout: Duration,
) -> Result<(Vec<u8>, Vec<u8>), ClientCommandError>
where
    Request: Serialize,
{
    validate_connection(destination, timeout)?;
    let request = serde_json::to_vec(request).map_err(ClientCommandError::EncodeControlRequest)?;
    if request.len() as u64 > MAXIMUM_CONTROL_REQUEST_BYTES {
        return Err(ClientCommandError::ControlRequestTooLarge);
    }

    let mut command = Command::new(program);
    configure_ssh_command(&mut command, destination, operation.remote_command);
    let mut child = command.spawn().map_err(ClientCommandError::StartSsh)?;
    let Some(mut stdin) = child.stdin.take() else {
        terminate_process_group(&mut child);
        return Err(ClientCommandError::MissingSshPipe);
    };
    let Some(mut stdout) = child.stdout.take() else {
        terminate_process_group(&mut child);
        return Err(ClientCommandError::MissingSshPipe);
    };
    let Some(mut stderr) = child.stderr.take() else {
        terminate_process_group(&mut child);
        return Err(ClientCommandError::MissingSshPipe);
    };

    let (events, event_receiver) = mpsc::sync_channel(3);
    let input_events = events.clone();
    let input = thread::spawn(move || {
        let result = stdin
            .write_all(&request)
            .map_err(ClientCommandError::WriteControlRequest);
        notify_transfer(&input_events, TransferEvent::Archive);
        result
    });
    let response_events = events.clone();
    let response = thread::spawn(move || {
        let result = read_bounded_control_response(&mut stdout, operation.maximum_response_bytes);
        notify_transfer(
            &response_events,
            TransferEvent::Response {
                failed: result.is_err(),
            },
        );
        result
    });
    let diagnostic = thread::spawn(move || {
        let result = read_bounded_diagnostic(&mut stderr);
        notify_transfer(
            &events,
            TransferEvent::Diagnostic {
                failed: result.is_err(),
            },
        );
        result
    });

    let wait_result = supervise_transfer(&mut child, timeout, &event_receiver);
    let input_result = input
        .join()
        .map_err(|_| ClientCommandError::ControlRequestThreadPanicked)
        .and_then(std::convert::identity);
    let response = response
        .join()
        .map_err(|_| ClientCommandError::ResponseThreadPanicked)
        .and_then(std::convert::identity)?;
    let diagnostic = diagnostic
        .join()
        .map_err(|_| ClientCommandError::DiagnosticThreadPanicked)
        .and_then(std::convert::identity)?;
    let status = wait_result?;
    if !status.success() {
        if let Some(response) = decode_gateway_error(&diagnostic)? {
            return Err(ClientCommandError::GatewayRejected(response));
        }
        return Err(ClientCommandError::SshFailed { status, diagnostic });
    }
    input_result?;
    Ok((response, diagnostic))
}

fn validate_connection(destination: &OsStr, timeout: Duration) -> Result<(), ClientCommandError> {
    if destination.is_empty() {
        Err(ClientCommandError::EmptyDestination)
    } else if timeout.is_zero() {
        Err(ClientCommandError::InvalidTimeout)
    } else {
        Ok(())
    }
}

fn write_json_response(
    mut output: impl Write,
    response: &impl Serialize,
) -> Result<(), ClientCommandError> {
    serde_json::to_writer(&mut output, response).map_err(ClientCommandError::EncodeResponse)?;
    output.write_all(b"\n").map_err(ClientCommandError::Output)
}

fn export_with_program(
    program: &OsStr,
    destination: &OsStr,
    request: &ExportRequest,
    timeout: Duration,
    mut output: impl Write,
    mut diagnostic_output: impl Write,
) -> Result<(), ClientCommandError> {
    let (archive, diagnostic) = execute_control_with_program(
        program,
        destination,
        ControlOperation::new(EXPORT_COMMAND, MAXIMUM_CONTROL_RESPONSE_BYTES),
        request,
        timeout,
    )?;
    validate_export_archive(&archive, request.document_id)?;
    diagnostic_output
        .write_all(&diagnostic)
        .map_err(ClientCommandError::DiagnosticOutput)?;
    output
        .write_all(&archive)
        .map_err(ClientCommandError::Output)
}

fn validate_export_archive(
    archive: &[u8],
    expected_document_id: DocumentId,
) -> Result<(), ClientCommandError> {
    let policy = PackagePolicy::default();
    let limits = policy.limits();
    let mut reader = tar::Archive::new(archive);
    let entries = reader
        .entries()
        .map_err(ClientCommandError::InvalidExportArchive)?;
    let mut names = std::collections::HashSet::new();
    let mut total_bytes = 0_u64;
    let mut document_id = None;
    let mut previous_attachment = None::<PathBuf>;
    let comparison = CanonicalArchiveWriter::new(archive);
    let mut canonical = Builder::new(comparison);
    canonical.mode(tar::HeaderMode::Deterministic);
    for (index, entry) in entries.enumerate() {
        if index >= limits.maximum_file_count {
            return Err(ClientCommandError::InvalidExportArchive(io::Error::other(
                "document bundle has too many entries",
            )));
        }
        let mut entry = entry.map_err(ClientCommandError::InvalidExportArchive)?;
        if entry.header().entry_type() != EntryType::Regular {
            return Err(ClientCommandError::InvalidExportArchive(io::Error::other(
                "document bundle contains a non-regular entry",
            )));
        }
        let path = entry
            .path()
            .map_err(ClientCommandError::InvalidExportArchive)?
            .into_owned();
        if path.components().count() != 1 || !names.insert(path.clone()) {
            return Err(ClientCommandError::InvalidExportArchive(io::Error::other(
                "document bundle contains an invalid or duplicate path",
            )));
        }
        let size = entry.size();
        if size > limits.maximum_file_bytes {
            return Err(ClientCommandError::InvalidExportArchive(io::Error::other(
                "document bundle entry exceeds the file limit",
            )));
        }
        total_bytes = total_bytes
            .checked_add(size)
            .filter(|total| *total <= limits.maximum_total_bytes)
            .ok_or_else(|| {
                ClientCommandError::InvalidExportArchive(io::Error::other(
                    "document bundle exceeds the byte limit",
                ))
            })?;
        if index == 0 && path != Path::new("index.md") {
            return Err(ClientCommandError::InvalidExportArchive(io::Error::other(
                "document bundle must begin with index.md",
            )));
        }
        let mut header = regular_export_header(size);
        if path != Path::new("index.md") {
            let valid_name = path
                .to_str()
                .is_some_and(|name| name.parse::<AttachmentName>().is_ok())
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| policy.allows_attachment_name(name));
            if !valid_name {
                return Err(ClientCommandError::InvalidExportArchive(io::Error::other(
                    "document bundle contains an unsupported attachment name",
                )));
            }
            if previous_attachment
                .as_ref()
                .is_some_and(|previous| previous >= &path)
            {
                return Err(ClientCommandError::InvalidExportArchive(io::Error::other(
                    "document bundle attachments are not in deterministic name order",
                )));
            }
            previous_attachment = Some(path.clone());
            canonical
                .append_data(&mut header, &path, &mut entry)
                .map_err(ClientCommandError::InvalidExportArchive)?;
            continue;
        }
        let mut markdown = Vec::with_capacity(usize::try_from(size).unwrap_or(0));
        entry
            .read_to_end(&mut markdown)
            .map_err(ClientCommandError::InvalidExportArchive)?;
        if markdown.len() as u64 != size {
            return Err(ClientCommandError::InvalidExportArchive(io::Error::other(
                "document bundle index is truncated",
            )));
        }
        let metadata = decode_document_metadata(&markdown, limits.maximum_front_matter_bytes)
            .map_err(|error| ClientCommandError::InvalidExportArchive(io::Error::other(error)))?;
        document_id = Some(metadata.document_id);
        canonical
            .append_data(&mut header, &path, markdown.as_slice())
            .map_err(ClientCommandError::InvalidExportArchive)?;
    }
    canonical
        .finish()
        .map_err(ClientCommandError::InvalidExportArchive)?;
    let comparison = canonical
        .into_inner()
        .map_err(ClientCommandError::InvalidExportArchive)?;
    if !comparison.is_complete() {
        return Err(ClientCommandError::InvalidExportArchive(io::Error::other(
            "document bundle contains noncanonical trailing data",
        )));
    }
    if document_id != Some(expected_document_id) {
        return Err(ClientCommandError::ExportDocumentMismatch);
    }
    Ok(())
}

fn regular_export_header(size: u64) -> Header {
    let mut header = Header::new_gnu();
    header.set_entry_type(EntryType::Regular);
    header.set_mode(0o644);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header.set_size(size);
    header.set_cksum();
    header
}

struct CanonicalArchiveWriter<'a> {
    expected: &'a [u8],
    position: usize,
}

impl<'a> CanonicalArchiveWriter<'a> {
    const fn new(expected: &'a [u8]) -> Self {
        Self {
            expected,
            position: 0,
        }
    }

    fn is_complete(&self) -> bool {
        self.position == self.expected.len()
    }
}

impl Write for CanonicalArchiveWriter<'_> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let end = self
            .position
            .checked_add(buffer.len())
            .ok_or_else(|| io::Error::other("canonical archive position overflowed"))?;
        if self.expected.get(self.position..end) != Some(buffer) {
            return Err(io::Error::other(
                "document bundle is not canonically encoded",
            ));
        }
        self.position = end;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn configure_ssh_command(command: &mut Command, destination: &OsStr, remote_command: &str) {
    command
        .arg("-T")
        .arg("-o")
        .arg("BatchMode=yes")
        .arg("-o")
        .arg("ClearAllForwardings=yes")
        .arg("-o")
        .arg("ForwardAgent=no")
        .arg("-o")
        .arg("ForwardX11=no")
        .arg("-o")
        .arg("StdinNull=no")
        .arg("-o")
        .arg("ForkAfterAuthentication=no")
        .arg("--")
        .arg(destination)
        .arg(remote_command)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_process_group(command);
}

fn submit_with_program(
    program: &OsStr,
    destination: &OsStr,
    package_root: &Path,
    timeout: Duration,
    mut output: impl Write,
    mut diagnostic_output: impl Write,
) -> Result<(), ClientCommandError> {
    let (response, diagnostic) =
        submit_response_with_program(program, destination, package_root, timeout)?;
    diagnostic_output
        .write_all(&diagnostic)
        .map_err(ClientCommandError::DiagnosticOutput)?;
    write_json_response(&mut output, &response)
}

fn submit_response_with_program(
    program: &OsStr,
    destination: &OsStr,
    package_root: &Path,
    timeout: Duration,
) -> Result<(SubmitResponse, Vec<u8>), ClientCommandError> {
    validate_connection(destination, timeout)?;

    let package = PreparedPackage::open(package_root)?;
    let expectation = package.expectation();
    let mut command = Command::new(program);
    configure_ssh_command(&mut command, destination, SUBMIT_COMMAND);
    let mut child = command.spawn().map_err(ClientCommandError::StartSsh)?;
    let Some(stdin) = child.stdin.take() else {
        terminate_process_group(&mut child);
        return Err(ClientCommandError::MissingSshPipe);
    };
    let Some(mut stdout) = child.stdout.take() else {
        terminate_process_group(&mut child);
        return Err(ClientCommandError::MissingSshPipe);
    };
    let Some(mut stderr) = child.stderr.take() else {
        terminate_process_group(&mut child);
        return Err(ClientCommandError::MissingSshPipe);
    };

    let (events, event_receiver) = mpsc::sync_channel(3);
    let archive_events = events.clone();
    let archive = thread::spawn(move || {
        let result = package.write_archive(stdin);
        notify_transfer(&archive_events, TransferEvent::Archive);
        result
    });
    let response_events = events.clone();
    let response = thread::spawn(move || {
        let result = read_bounded_response(&mut stdout);
        notify_transfer(
            &response_events,
            TransferEvent::Response {
                failed: result.is_err(),
            },
        );
        result
    });
    let diagnostic = thread::spawn(move || {
        let result = read_bounded_diagnostic(&mut stderr);
        notify_transfer(
            &events,
            TransferEvent::Diagnostic {
                failed: result.is_err(),
            },
        );
        result
    });
    let wait_result = supervise_transfer(&mut child, timeout, &event_receiver);
    let archive_result = archive
        .join()
        .map_err(|_| ClientCommandError::ArchiveThreadPanicked)
        .and_then(std::convert::identity);
    let response = response
        .join()
        .map_err(|_| ClientCommandError::ResponseThreadPanicked)
        .and_then(std::convert::identity)?;
    let diagnostic = diagnostic
        .join()
        .map_err(|_| ClientCommandError::DiagnosticThreadPanicked)
        .and_then(std::convert::identity)?;
    let status = wait_result?;

    if !status.success() {
        if let Some(response) = decode_gateway_error(&diagnostic)? {
            return Err(ClientCommandError::GatewayRejected(response));
        }
        return Err(ClientCommandError::SshFailed { status, diagnostic });
    }
    archive_result?;

    let protocol_version =
        decode_protocol_version(&response).map_err(ClientCommandError::InvalidResponse)?;
    require_current_protocol(protocol_version)?;
    let response: SubmitResponse =
        serde_json::from_slice(&response).map_err(ClientCommandError::InvalidResponse)?;
    expectation.verify(&response)?;
    Ok((response, diagnostic))
}

#[cfg(unix)]
fn configure_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    command.process_group(0);
}

#[cfg(not(unix))]
fn configure_process_group(_command: &mut Command) {}

#[derive(Clone, Copy)]
enum TransferEvent {
    Archive,
    Response { failed: bool },
    Diagnostic { failed: bool },
}

fn notify_transfer(events: &SyncSender<TransferEvent>, event: TransferEvent) {
    let _ = events.send(event);
}

#[derive(Default)]
struct TransferProgress {
    archive_done: bool,
    response_done: bool,
    diagnostic_done: bool,
}

impl TransferProgress {
    fn observe(&mut self, event: TransferEvent) -> bool {
        match event {
            TransferEvent::Archive => self.archive_done = true,
            TransferEvent::Response { failed } => {
                self.response_done = true;
                return failed;
            }
            TransferEvent::Diagnostic { failed } => {
                self.diagnostic_done = true;
                return failed;
            }
        }
        false
    }

    const fn complete(&self) -> bool {
        self.archive_done && self.response_done && self.diagnostic_done
    }
}

fn supervise_transfer(
    child: &mut Child,
    timeout: Duration,
    events: &Receiver<TransferEvent>,
) -> Result<ExitStatus, ClientCommandError> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or(ClientCommandError::InvalidTimeout)?;
    let mut progress = TransferProgress::default();
    let mut exit_status = None;
    loop {
        if exit_status.is_none() {
            match child.try_wait() {
                Ok(status) => exit_status = status,
                Err(error) => {
                    terminate_process_group(child);
                    return Err(ClientCommandError::WaitForSsh(error));
                }
            }
        }
        if exit_status.is_some() && progress.complete() {
            return exit_status.ok_or(ClientCommandError::TransferState);
        }

        let now = Instant::now();
        if now >= deadline {
            terminate_process_group(child);
            return Err(ClientCommandError::SshTimedOut { timeout });
        }
        let wait = deadline
            .saturating_duration_since(now)
            .min(Duration::from_millis(10));
        match events.recv_timeout(wait) {
            Ok(event) if progress.observe(event) => {
                terminate_process_group(child);
                return Err(ClientCommandError::TransferCancelled);
            }
            Ok(_) | Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) if progress.complete() => thread::sleep(wait),
            Err(RecvTimeoutError::Disconnected) => {
                terminate_process_group(child);
                return Err(ClientCommandError::TransferState);
            }
        }
    }
}

fn terminate_process_group(child: &mut Child) {
    #[cfg(unix)]
    if let Ok(process_group) = i32::try_from(child.id()) {
        use nix::sys::signal::{Signal, killpg};
        use nix::unistd::Pid;

        let _ = killpg(Pid::from_raw(process_group), Signal::SIGKILL);
    }
    let _ = child.kill();
    let _ = child.wait();
}

#[derive(Deserialize)]
struct ProtocolEnvelope {
    protocol_version: u16,
    #[serde(flatten)]
    _remaining: std::collections::BTreeMap<String, serde_json::Value>,
}

fn decode_protocol_version(bytes: &[u8]) -> Result<u16, serde_json::Error> {
    serde_json::from_slice::<ProtocolEnvelope>(bytes).map(|envelope| envelope.protocol_version)
}

fn require_current_protocol(protocol_version: u16) -> Result<(), ClientCommandError> {
    if protocol_version != CURRENT_GATEWAY_PROTOCOL_VERSION {
        return Err(ClientCommandError::UnsupportedProtocolVersion {
            actual: protocol_version,
        });
    }
    Ok(())
}

fn decode_gateway_error(
    diagnostic: &[u8],
) -> Result<Option<ProtocolErrorResponse>, ClientCommandError> {
    let Some(line) = diagnostic
        .split(|byte| *byte == b'\n')
        .rev()
        .map(trim_ascii_whitespace)
        .find(|line| !line.is_empty())
    else {
        return Ok(None);
    };
    let protocol_version = match decode_protocol_version(line) {
        Ok(protocol_version) => protocol_version,
        Err(_) => return Ok(None),
    };
    require_current_protocol(protocol_version)?;
    Ok(serde_json::from_slice(line).ok())
}

fn trim_ascii_whitespace(mut bytes: &[u8]) -> &[u8] {
    while bytes.first().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[1..];
    }
    while bytes.last().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[..bytes.len() - 1];
    }
    bytes
}

fn read_bounded_response(input: &mut impl Read) -> Result<Vec<u8>, ClientCommandError> {
    let mut response = Vec::new();
    input
        .take(MAXIMUM_RESPONSE_BYTES.saturating_add(1))
        .read_to_end(&mut response)
        .map_err(ClientCommandError::ReadResponse)?;
    if response.len() as u64 > MAXIMUM_RESPONSE_BYTES {
        return Err(ClientCommandError::ResponseTooLarge);
    }
    Ok(response)
}

fn read_bounded_control_response(
    input: &mut impl Read,
    maximum_bytes: u64,
) -> Result<Vec<u8>, ClientCommandError> {
    let mut response = Vec::new();
    input
        .take(maximum_bytes.saturating_add(1))
        .read_to_end(&mut response)
        .map_err(ClientCommandError::ReadResponse)?;
    if response.len() as u64 > maximum_bytes {
        return Err(ClientCommandError::ControlResponseTooLarge {
            maximum: maximum_bytes,
        });
    }
    Ok(response)
}

fn read_bounded_diagnostic(input: &mut impl Read) -> Result<Vec<u8>, ClientCommandError> {
    let mut diagnostic = Vec::new();
    input
        .take(MAXIMUM_RESPONSE_BYTES.saturating_add(1))
        .read_to_end(&mut diagnostic)
        .map_err(ClientCommandError::ReadDiagnostic)?;
    if diagnostic.len() as u64 > MAXIMUM_RESPONSE_BYTES {
        return Err(ClientCommandError::DiagnosticTooLarge);
    }
    Ok(diagnostic)
}

struct PreparedPackage {
    request: Vec<u8>,
    payload: Vec<PreparedPayload>,
    request_id: RequestId,
    digest: Revision,
}

impl PreparedPackage {
    fn open(package_root: &Path) -> Result<Self, ClientCommandError> {
        let validated = validate_package(package_root, &PackagePolicy::default())
            .map_err(ClientCommandError::PackageValidation)?;
        let request =
            serde_json::to_vec(validated.request()).map_err(ClientCommandError::EncodeRequest)?;
        let root = PinnedDirectory::open(package_root).map_err(ClientCommandError::OpenPackage)?;
        let payload = prepare_payload(&root, &validated)?;
        Ok(Self {
            request,
            payload,
            request_id: validated.request().request_id,
            digest: validated.digest().as_revision(),
        })
    }

    const fn expectation(&self) -> SubmissionExpectation {
        SubmissionExpectation {
            request_id: self.request_id,
            digest: self.digest,
        }
    }

    fn write_archive(self, output: impl Write) -> Result<(), ClientCommandError> {
        let mut archive = Builder::new(output);
        archive.follow_symlinks(false);
        archive.sparse(false);
        append_bytes(&mut archive, "request.json", &self.request)?;
        append_directory(&mut archive, "payload")?;
        for payload in self.payload {
            let archive_path = Path::new("payload").join(&payload.path);
            let mut header = regular_header(payload.bytes.len() as u64);
            archive
                .append_data(&mut header, archive_path, payload.bytes.as_slice())
                .map_err(ClientCommandError::WriteArchive)?;
        }
        archive.finish().map_err(ClientCommandError::WriteArchive)
    }
}

struct PreparedPayload {
    path: PathBuf,
    bytes: Vec<u8>,
}

fn prepare_payload(
    package_root: &PinnedDirectory,
    package: &ValidatedPackage,
) -> Result<Vec<PreparedPayload>, ClientCommandError> {
    package
        .payload()
        .iter()
        .map(|metadata| open_payload(package_root, metadata))
        .collect()
}

fn open_payload(
    package_root: &PinnedDirectory,
    metadata: &PayloadMetadata,
) -> Result<PreparedPayload, ClientCommandError> {
    let relative = PathBuf::from(metadata.path().as_str());
    let mut file = package_root
        .open_regular_beneath(Path::new("payload").join(&relative))
        .map_err(|source| ClientCommandError::OpenPayload {
            path: relative.clone(),
            source,
        })?;
    if file.byte_length() != metadata.byte_length() {
        return Err(ClientCommandError::PackageChanged { path: relative });
    }
    let capacity = usize::try_from(metadata.byte_length()).unwrap_or(usize::MAX);
    let mut bytes = Vec::with_capacity(capacity);
    file.by_ref()
        .take(metadata.byte_length().saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|source| ClientCommandError::ReadPayload {
            path: relative.clone(),
            source,
        })?;
    let revision = Revision::from_bytes(Sha256::digest(&bytes).into());
    if bytes.len() as u64 != metadata.byte_length() || revision != metadata.revision() {
        return Err(ClientCommandError::PackageChanged { path: relative });
    }
    Ok(PreparedPayload {
        path: relative,
        bytes,
    })
}

fn regular_header(size: u64) -> Header {
    let mut header = Header::new_gnu();
    header.set_entry_type(EntryType::Regular);
    header.set_mode(0o644);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header.set_size(size);
    header.set_cksum();
    header
}

fn append_bytes(
    archive: &mut Builder<impl Write>,
    path: &str,
    bytes: &[u8],
) -> Result<(), ClientCommandError> {
    let mut header = regular_header(bytes.len() as u64);
    archive
        .append_data(&mut header, path, bytes)
        .map_err(ClientCommandError::WriteArchive)
}

fn append_directory(
    archive: &mut Builder<impl Write>,
    path: &str,
) -> Result<(), ClientCommandError> {
    let mut header = Header::new_gnu();
    header.set_entry_type(EntryType::Directory);
    header.set_mode(0o755);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header.set_size(0);
    header.set_cksum();
    archive
        .append_data(&mut header, path, io::empty())
        .map_err(ClientCommandError::WriteArchive)
}

#[derive(Clone, Copy)]
struct SubmissionExpectation {
    request_id: RequestId,
    digest: Revision,
}

impl SubmissionExpectation {
    fn verify(self, response: &SubmitResponse) -> Result<(), ClientCommandError> {
        let (request_id, digest) = match response.outcome {
            SubmitOutcome::Accepted { request_id, digest }
            | SubmitOutcome::Existing {
                request_id, digest, ..
            } => (request_id, digest),
        };
        if request_id != self.request_id || digest != self.digest {
            return Err(ClientCommandError::ResponseMismatch);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum ClientCommandError {
    EmptyDestination,
    PackageValidation(PackageValidationError),
    EncodeRequest(serde_json::Error),
    OpenPackage(PinnedPathError),
    OpenPayload {
        path: PathBuf,
        source: PinnedPathError,
    },
    ReadPayload {
        path: PathBuf,
        source: io::Error,
    },
    PackageChanged {
        path: PathBuf,
    },
    StartSsh(io::Error),
    MissingSshPipe,
    WriteArchive(io::Error),
    ArchiveThreadPanicked,
    EncodeControlRequest(serde_json::Error),
    ControlRequestTooLarge,
    WriteControlRequest(io::Error),
    ControlRequestThreadPanicked,
    ResponseThreadPanicked,
    DiagnosticThreadPanicked,
    ReadResponse(io::Error),
    ResponseTooLarge,
    ControlResponseTooLarge {
        maximum: u64,
    },
    ReadDiagnostic(io::Error),
    DiagnosticTooLarge,
    TransferCancelled,
    TransferState,
    WaitForSsh(io::Error),
    InvalidTimeout,
    SshTimedOut {
        timeout: Duration,
    },
    SshFailed {
        status: ExitStatus,
        diagnostic: Vec<u8>,
    },
    GatewayRejected(ProtocolErrorResponse),
    InvalidResponse(serde_json::Error),
    UnsupportedProtocolVersion {
        actual: u16,
    },
    ResponseMismatch,
    DocumentResponseMismatch,
    InvalidExportArchive(io::Error),
    ExportDocumentMismatch,
    RequestStatusResponseMismatch,
    DiagnosticOutput(io::Error),
    EncodeResponse(serde_json::Error),
    Output(io::Error),
}

impl ClientCommandError {
    pub fn write_diagnostic(&self, mut output: impl Write) -> io::Result<()> {
        match self {
            Self::GatewayRejected(response) => {
                serde_json::to_writer(&mut output, response).map_err(io::Error::other)?;
                output.write_all(b"\n")
            }
            Self::SshFailed { diagnostic, .. } => {
                output.write_all(diagnostic)?;
                if !diagnostic.is_empty() && !diagnostic.ends_with(b"\n") {
                    output.write_all(b"\n")?;
                }
                writeln!(output, "{self}")
            }
            _ => writeln!(output, "{self}"),
        }
    }
}

impl fmt::Display for ClientCommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyDestination => formatter.write_str("SSH destination must not be empty"),
            Self::PackageValidation(error) => {
                write!(formatter, "package validation failed: {error}")
            }
            Self::EncodeRequest(error) => write!(formatter, "request encoding failed: {error}"),
            Self::OpenPackage(error) => {
                write!(formatter, "could not pin package directory: {error}")
            }
            Self::OpenPayload { path, source } => write!(
                formatter,
                "could not pin payload `{}`: {source}",
                path.display()
            ),
            Self::ReadPayload { path, source } => write!(
                formatter,
                "could not snapshot payload `{}`: {source}",
                path.display()
            ),
            Self::PackageChanged { path } => write!(
                formatter,
                "payload `{}` changed after package validation",
                path.display()
            ),
            Self::StartSsh(error) => write!(formatter, "could not start ssh: {error}"),
            Self::MissingSshPipe => formatter.write_str("ssh did not provide a requested pipe"),
            Self::WriteArchive(error) => {
                write!(formatter, "could not stream package archive: {error}")
            }
            Self::ArchiveThreadPanicked => formatter.write_str("package archive writer panicked"),
            Self::EncodeControlRequest(error) => {
                write!(formatter, "control request encoding failed: {error}")
            }
            Self::ControlRequestTooLarge => write!(
                formatter,
                "control request exceeds {MAXIMUM_CONTROL_REQUEST_BYTES} bytes"
            ),
            Self::WriteControlRequest(error) => {
                write!(
                    formatter,
                    "could not write Gateway control request: {error}"
                )
            }
            Self::ControlRequestThreadPanicked => {
                formatter.write_str("Gateway control request writer panicked")
            }
            Self::ResponseThreadPanicked => formatter.write_str("ssh response reader panicked"),
            Self::DiagnosticThreadPanicked => formatter.write_str("ssh diagnostic reader panicked"),
            Self::ReadResponse(error) => {
                write!(formatter, "could not read Gateway response: {error}")
            }
            Self::ResponseTooLarge => write!(
                formatter,
                "Gateway response exceeds {MAXIMUM_RESPONSE_BYTES} bytes"
            ),
            Self::ControlResponseTooLarge { maximum } => write!(
                formatter,
                "Gateway control response exceeds {maximum} bytes"
            ),
            Self::ReadDiagnostic(error) => {
                write!(formatter, "could not read ssh diagnostics: {error}")
            }
            Self::DiagnosticTooLarge => write!(
                formatter,
                "ssh diagnostics exceed {MAXIMUM_RESPONSE_BYTES} bytes"
            ),
            Self::TransferCancelled => {
                formatter.write_str("ssh transfer was cancelled after a stream failure")
            }
            Self::TransferState => formatter.write_str("ssh transfer state became inconsistent"),
            Self::WaitForSsh(error) => write!(formatter, "could not wait for ssh: {error}"),
            Self::InvalidTimeout => formatter.write_str("SSH timeout must be positive"),
            Self::SshTimedOut { timeout } => {
                write!(
                    formatter,
                    "ssh timed out after {} seconds",
                    timeout.as_secs()
                )
            }
            Self::SshFailed { status, .. } => {
                write!(formatter, "ssh exited unsuccessfully ({status})")
            }
            Self::GatewayRejected(response) => {
                write!(
                    formatter,
                    "Gateway rejected the request ({})",
                    response.error_code
                )
            }
            Self::InvalidResponse(error) => {
                write!(formatter, "Gateway response is invalid: {error}")
            }
            Self::UnsupportedProtocolVersion { actual } => write!(
                formatter,
                "Gateway protocol version {actual} is unsupported; expected {CURRENT_GATEWAY_PROTOCOL_VERSION}"
            ),
            Self::ResponseMismatch => formatter.write_str(
                "Gateway response request ID or digest does not match the submitted package",
            ),
            Self::DocumentResponseMismatch => {
                formatter.write_str("Gateway response document ID does not match the request")
            }
            Self::InvalidExportArchive(error) => {
                write!(formatter, "Gateway export archive is invalid: {error}")
            }
            Self::ExportDocumentMismatch => {
                formatter.write_str("Gateway export document ID does not match the request")
            }
            Self::RequestStatusResponseMismatch => {
                formatter.write_str("Gateway status response request ID does not match the request")
            }
            Self::DiagnosticOutput(error) => {
                write!(formatter, "could not write ssh diagnostics: {error}")
            }
            Self::EncodeResponse(error) => write!(formatter, "response encoding failed: {error}"),
            Self::Output(error) => write!(formatter, "could not write command output: {error}"),
        }
    }
}

impl std::error::Error for ClientCommandError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::PackageValidation(error) => Some(error),
            Self::EncodeRequest(error)
            | Self::EncodeControlRequest(error)
            | Self::InvalidResponse(error)
            | Self::EncodeResponse(error) => Some(error),
            Self::OpenPackage(error) => Some(error),
            Self::OpenPayload { source, .. } => Some(source),
            Self::ReadPayload { source, .. }
            | Self::StartSsh(source)
            | Self::WriteArchive(source)
            | Self::WriteControlRequest(source)
            | Self::ReadResponse(source)
            | Self::ReadDiagnostic(source)
            | Self::InvalidExportArchive(source)
            | Self::WaitForSsh(source)
            | Self::DiagnosticOutput(source)
            | Self::Output(source) => Some(source),
            Self::EmptyDestination
            | Self::PackageChanged { .. }
            | Self::MissingSshPipe
            | Self::ArchiveThreadPanicked
            | Self::ControlRequestTooLarge
            | Self::ControlRequestThreadPanicked
            | Self::ResponseThreadPanicked
            | Self::DiagnosticThreadPanicked
            | Self::ResponseTooLarge
            | Self::ControlResponseTooLarge { .. }
            | Self::DiagnosticTooLarge
            | Self::TransferCancelled
            | Self::TransferState
            | Self::InvalidTimeout
            | Self::SshTimedOut { .. }
            | Self::SshFailed { .. }
            | Self::GatewayRejected(_)
            | Self::UnsupportedProtocolVersion { .. }
            | Self::ExportDocumentMismatch
            | Self::ResponseMismatch
            | Self::DocumentResponseMismatch
            | Self::RequestStatusResponseMismatch => None,
        }
    }
}

#[cfg(test)]
mod tests;

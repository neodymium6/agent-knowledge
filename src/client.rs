use std::ffi::OsStr;
use std::fmt;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use agent_knowledge_core::{BoundedFileError, PinnedRegularFile, RequestId, Revision};
use agent_knowledge_protocol::{
    CURRENT_GATEWAY_PROTOCOL_VERSION, ProtocolErrorResponse, SUBMIT_COMMAND, SubmitOutcome,
    SubmitResponse,
};
use agent_knowledge_queue::{
    PackagePolicy, PackageValidationError, PayloadMetadata, ValidatedPackage, validate_package,
};
use sha2::{Digest, Sha256};
use tar::{Builder, EntryType, Header};

const SSH_PROGRAM: &str = "ssh";
const MAXIMUM_RESPONSE_BYTES: u64 = 64 * 1024;

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

fn submit_with_program(
    program: &OsStr,
    destination: &OsStr,
    package_root: &Path,
    timeout: Duration,
    mut output: impl Write,
    mut diagnostic_output: impl Write,
) -> Result<(), ClientCommandError> {
    if destination.is_empty() {
        return Err(ClientCommandError::EmptyDestination);
    }
    if timeout.is_zero() {
        return Err(ClientCommandError::InvalidTimeout);
    }

    let package = PreparedPackage::open(package_root)?;
    let expectation = package.expectation();
    let mut child = Command::new(program)
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
        .arg(SUBMIT_COMMAND)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(ClientCommandError::StartSsh)?;
    let Some(stdin) = child.stdin.take() else {
        terminate(&mut child);
        return Err(ClientCommandError::MissingSshPipe);
    };
    let Some(mut stdout) = child.stdout.take() else {
        terminate(&mut child);
        return Err(ClientCommandError::MissingSshPipe);
    };
    let Some(mut stderr) = child.stderr.take() else {
        terminate(&mut child);
        return Err(ClientCommandError::MissingSshPipe);
    };

    let archive = thread::spawn(move || package.write_archive(stdin));
    let response = thread::spawn(move || read_bounded_response(&mut stdout));
    let diagnostic = thread::spawn(move || read_bounded_diagnostic(&mut stderr));
    let wait_result = wait_with_deadline(&mut child, timeout);
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
        if let Ok(response) = serde_json::from_slice::<ProtocolErrorResponse>(&diagnostic) {
            if response.protocol_version != CURRENT_GATEWAY_PROTOCOL_VERSION {
                return Err(ClientCommandError::UnsupportedProtocolVersion {
                    actual: response.protocol_version,
                });
            }
            return Err(ClientCommandError::GatewayRejected(response));
        }
        return Err(ClientCommandError::SshFailed { status, diagnostic });
    }
    archive_result?;

    let response: SubmitResponse =
        serde_json::from_slice(&response).map_err(ClientCommandError::InvalidResponse)?;
    if response.protocol_version != CURRENT_GATEWAY_PROTOCOL_VERSION {
        return Err(ClientCommandError::UnsupportedProtocolVersion {
            actual: response.protocol_version,
        });
    }
    expectation.verify(&response)?;
    diagnostic_output
        .write_all(&diagnostic)
        .map_err(ClientCommandError::DiagnosticOutput)?;
    serde_json::to_writer(&mut output, &response).map_err(ClientCommandError::EncodeResponse)?;
    output.write_all(b"\n").map_err(ClientCommandError::Output)
}

fn wait_with_deadline(
    child: &mut Child,
    timeout: Duration,
) -> Result<ExitStatus, ClientCommandError> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or(ClientCommandError::InvalidTimeout)?;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Ok(status),
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(10)),
            Ok(None) => {
                terminate(child);
                return Err(ClientCommandError::SshTimedOut { timeout });
            }
            Err(error) => {
                terminate(child);
                return Err(ClientCommandError::WaitForSsh(error));
            }
        }
    }
}

fn terminate(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
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
        let payload = prepare_payload(package_root, &validated)?;
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
    package_root: &Path,
    package: &ValidatedPackage,
) -> Result<Vec<PreparedPayload>, ClientCommandError> {
    package
        .payload()
        .iter()
        .map(|metadata| open_payload(package_root, metadata))
        .collect()
}

fn open_payload(
    package_root: &Path,
    metadata: &PayloadMetadata,
) -> Result<PreparedPayload, ClientCommandError> {
    let relative = PathBuf::from(metadata.path().as_str());
    let mut file = PinnedRegularFile::open_no_follow(package_root.join("payload").join(&relative))
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
pub(crate) enum ClientCommandError {
    EmptyDestination,
    PackageValidation(PackageValidationError),
    EncodeRequest(serde_json::Error),
    OpenPayload {
        path: PathBuf,
        source: BoundedFileError,
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
    ResponseThreadPanicked,
    DiagnosticThreadPanicked,
    ReadResponse(io::Error),
    ResponseTooLarge,
    ReadDiagnostic(io::Error),
    DiagnosticTooLarge,
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
            Self::ResponseThreadPanicked => formatter.write_str("ssh response reader panicked"),
            Self::DiagnosticThreadPanicked => formatter.write_str("ssh diagnostic reader panicked"),
            Self::ReadResponse(error) => {
                write!(formatter, "could not read Gateway response: {error}")
            }
            Self::ResponseTooLarge => write!(
                formatter,
                "Gateway response exceeds {MAXIMUM_RESPONSE_BYTES} bytes"
            ),
            Self::ReadDiagnostic(error) => {
                write!(formatter, "could not read ssh diagnostics: {error}")
            }
            Self::DiagnosticTooLarge => write!(
                formatter,
                "ssh diagnostics exceed {MAXIMUM_RESPONSE_BYTES} bytes"
            ),
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
            | Self::InvalidResponse(error)
            | Self::EncodeResponse(error) => Some(error),
            Self::OpenPayload { source, .. } => Some(source),
            Self::ReadPayload { source, .. }
            | Self::StartSsh(source)
            | Self::WriteArchive(source)
            | Self::ReadResponse(source)
            | Self::ReadDiagnostic(source)
            | Self::WaitForSsh(source)
            | Self::DiagnosticOutput(source)
            | Self::Output(source) => Some(source),
            Self::EmptyDestination
            | Self::PackageChanged { .. }
            | Self::MissingSshPipe
            | Self::ArchiveThreadPanicked
            | Self::ResponseThreadPanicked
            | Self::DiagnosticThreadPanicked
            | Self::ResponseTooLarge
            | Self::DiagnosticTooLarge
            | Self::InvalidTimeout
            | Self::SshTimedOut { .. }
            | Self::SshFailed { .. }
            | Self::GatewayRejected(_)
            | Self::UnsupportedProtocolVersion { .. }
            | Self::ResponseMismatch => None,
        }
    }
}

#[cfg(test)]
mod tests;

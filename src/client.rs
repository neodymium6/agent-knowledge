use std::ffi::OsStr;
use std::fmt;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};

use agent_knowledge_core::{BoundedFileError, PinnedRegularFile, Revision};
use agent_knowledge_protocol::{CURRENT_GATEWAY_PROTOCOL_VERSION, SUBMIT_COMMAND, SubmitResponse};
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
    output: impl Write,
) -> Result<(), ClientCommandError> {
    submit_with_program(OsStr::new(SSH_PROGRAM), destination, package_root, output)
}

fn submit_with_program(
    program: &OsStr,
    destination: &OsStr,
    package_root: &Path,
    mut output: impl Write,
) -> Result<(), ClientCommandError> {
    if destination.is_empty() {
        return Err(ClientCommandError::EmptyDestination);
    }

    let package = PreparedPackage::open(package_root)?;
    let mut child = Command::new(program)
        .arg("-T")
        .arg("-o")
        .arg("BatchMode=yes")
        .arg("-o")
        .arg("ClearAllForwardings=yes")
        .arg("--")
        .arg(destination)
        .arg(SUBMIT_COMMAND)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
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

    let (response, status, archive_result) = std::thread::scope(|scope| {
        let archive = scope.spawn(move || package.write_archive(stdin));
        let response = read_bounded_response(&mut stdout);
        if response.is_err() {
            let _ = child.kill();
        }
        let status = child.wait().map_err(ClientCommandError::WaitForSsh);
        let archive_result = archive
            .join()
            .map_err(|_| ClientCommandError::ArchiveThreadPanicked)
            .and_then(std::convert::identity);
        (response, status, archive_result)
    });

    let status = status?;
    let response = response?;
    if !status.success() {
        return Err(ClientCommandError::SshFailed(status));
    }
    archive_result?;
    let response: SubmitResponse =
        serde_json::from_slice(&response).map_err(ClientCommandError::InvalidResponse)?;
    if response.protocol_version != CURRENT_GATEWAY_PROTOCOL_VERSION {
        return Err(ClientCommandError::UnsupportedProtocolVersion {
            actual: response.protocol_version,
        });
    }
    serde_json::to_writer(&mut output, &response).map_err(ClientCommandError::EncodeResponse)?;
    output.write_all(b"\n").map_err(ClientCommandError::Output)
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

struct PreparedPackage {
    request: Vec<u8>,
    payload: Vec<PreparedPayload>,
}

impl PreparedPackage {
    fn open(package_root: &Path) -> Result<Self, ClientCommandError> {
        let validated = validate_package(package_root, &PackagePolicy::default())
            .map_err(ClientCommandError::PackageValidation)?;
        let request =
            serde_json::to_vec(validated.request()).map_err(ClientCommandError::EncodeRequest)?;
        let payload = prepare_payload(package_root, &validated)?;
        Ok(Self { request, payload })
    }

    fn write_archive(self, output: impl Write) -> Result<(), ClientCommandError> {
        let mut archive = Builder::new(output);
        archive.follow_symlinks(false);
        archive.sparse(false);
        append_bytes(&mut archive, "request.json", &self.request)?;
        append_directory(&mut archive, "payload")?;
        for mut payload in self.payload {
            let mut header = regular_header(payload.byte_length);
            let archive_path = Path::new("payload").join(&payload.path);
            let mut verified = VerifiedReader::new(
                payload
                    .file
                    .by_ref()
                    .take(payload.byte_length.saturating_add(1)),
            );
            archive
                .append_data(&mut header, archive_path, &mut verified)
                .map_err(ClientCommandError::WriteArchive)?;
            let (actual_length, actual_revision) = verified.finish();
            if actual_length != payload.byte_length || actual_revision != payload.revision {
                return Err(ClientCommandError::PackageChanged { path: payload.path });
            }
        }
        archive.finish().map_err(ClientCommandError::WriteArchive)
    }
}

struct PreparedPayload {
    path: PathBuf,
    file: PinnedRegularFile,
    byte_length: u64,
    revision: Revision,
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
    let file = PinnedRegularFile::open(package_root.join("payload").join(&relative)).map_err(
        |source| ClientCommandError::OpenPayload {
            path: relative.clone(),
            source,
        },
    )?;
    if file.byte_length() != metadata.byte_length() {
        return Err(ClientCommandError::PackageChanged { path: relative });
    }
    Ok(PreparedPayload {
        path: relative,
        file,
        byte_length: metadata.byte_length(),
        revision: metadata.revision(),
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

struct VerifiedReader<R> {
    inner: R,
    digest: Sha256,
    byte_length: u64,
}

impl<R> VerifiedReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            digest: Sha256::new(),
            byte_length: 0,
        }
    }

    fn finish(self) -> (u64, Revision) {
        (
            self.byte_length,
            Revision::from_bytes(self.digest.finalize().into()),
        )
    }
}

impl<R: Read> Read for VerifiedReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buffer)?;
        self.digest.update(&buffer[..read]);
        self.byte_length = self.byte_length.saturating_add(read as u64);
        Ok(read)
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
    PackageChanged {
        path: PathBuf,
    },
    StartSsh(io::Error),
    MissingSshPipe,
    WriteArchive(io::Error),
    ArchiveThreadPanicked,
    ReadResponse(io::Error),
    ResponseTooLarge,
    WaitForSsh(io::Error),
    SshFailed(ExitStatus),
    InvalidResponse(serde_json::Error),
    UnsupportedProtocolVersion {
        actual: u16,
    },
    EncodeResponse(serde_json::Error),
    Output(io::Error),
}

impl fmt::Display for ClientCommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyDestination => formatter.write_str("SSH destination must not be empty"),
            Self::PackageValidation(error) => {
                write!(formatter, "package validation failed: {error}")
            }
            Self::EncodeRequest(error) => write!(formatter, "request encoding failed: {error}"),
            Self::OpenPayload { path, source } => {
                write!(
                    formatter,
                    "could not pin payload `{}`: {source}",
                    path.display()
                )
            }
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
            Self::ReadResponse(error) => {
                write!(formatter, "could not read Gateway response: {error}")
            }
            Self::ResponseTooLarge => write!(
                formatter,
                "Gateway response exceeds {MAXIMUM_RESPONSE_BYTES} bytes"
            ),
            Self::WaitForSsh(error) => write!(formatter, "could not wait for ssh: {error}"),
            Self::SshFailed(status) => write!(formatter, "ssh exited unsuccessfully ({status})"),
            Self::InvalidResponse(error) => {
                write!(formatter, "Gateway response is invalid: {error}")
            }
            Self::UnsupportedProtocolVersion { actual } => write!(
                formatter,
                "Gateway protocol version {actual} is unsupported; expected {CURRENT_GATEWAY_PROTOCOL_VERSION}"
            ),
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
            Self::StartSsh(error)
            | Self::WriteArchive(error)
            | Self::ReadResponse(error)
            | Self::WaitForSsh(error)
            | Self::Output(error) => Some(error),
            Self::EmptyDestination
            | Self::PackageChanged { .. }
            | Self::MissingSshPipe
            | Self::ArchiveThreadPanicked
            | Self::ResponseTooLarge
            | Self::SshFailed(_)
            | Self::UnsupportedProtocolVersion { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::fmt;
use std::io::{self, Read};

use agent_knowledge_core::{ErrorCode, PayloadPath};
use agent_knowledge_protocol::{ClientId, RequestState, SubmitOutcome, SubmitResponse};
use agent_knowledge_queue::{EnqueueOutcome, FileQueue, PackageLimits, QueueState};
use tar::{Archive, EntryType};

const TAR_BLOCK_BYTES: u64 = 512;
const MAXIMUM_PATH_EXTENSION_BYTES_PER_ENTRY: u64 = 8 * 1024;
const TRAILING_BUFFER_BYTES: usize = 16 * 1024;

pub(crate) fn submit(
    queue: &FileQueue,
    client_id: ClientId,
    input: impl Read,
) -> Result<SubmitResponse, super::GatewayError> {
    let mut incoming = queue
        .begin()
        .map_err(|error| super::GatewayError::Queue(Box::new(error)))?;
    let limits = queue.policy().limits();
    let maximum_archive_bytes = maximum_archive_bytes(limits);
    let limited = TransportReader(input).take(maximum_archive_bytes.saturating_add(1));
    let mut archive = Archive::new(limited);
    let mut seen = HashSet::new();
    let mut directories = HashSet::new();
    let mut payload_files = Vec::new();
    let mut request_seen = false;
    let mut entry_count = 0_usize;

    {
        let mut entries = archive
            .entries()
            .map_err(ArchiveError::io)
            .map_err(super::GatewayError::Archive)?;
        for entry in &mut entries {
            entry_count = entry_count
                .checked_add(1)
                .ok_or(ArchiveError::TooManyEntries {
                    maximum: limits.maximum_entry_count,
                })
                .map_err(super::GatewayError::Archive)?;
            if entry_count > limits.maximum_entry_count {
                return Err(super::GatewayError::Archive(ArchiveError::TooManyEntries {
                    maximum: limits.maximum_entry_count,
                }));
            }
            let mut entry = entry
                .map_err(ArchiveError::io)
                .map_err(super::GatewayError::Archive)?;
            entry
                .header()
                .cksum()
                .map_err(ArchiveError::io)
                .map_err(super::GatewayError::Archive)?;
            if entry
                .pax_extensions()
                .map_err(ArchiveError::io)
                .map_err(super::GatewayError::Archive)?
                .is_some()
            {
                return Err(super::GatewayError::Archive(
                    ArchiveError::UnsupportedExtension,
                ));
            }
            let entry_type = entry.header().entry_type();
            if entry_type.is_gnu_sparse() {
                return Err(super::GatewayError::Archive(ArchiveError::SparseEntry));
            }
            let path_bytes = entry.path_bytes();
            let path = std::str::from_utf8(path_bytes.as_ref())
                .map_err(|_| super::GatewayError::Archive(ArchiveError::InvalidPath))?;
            let path =
                normalize_entry_path(path, entry_type).map_err(super::GatewayError::Archive)?;
            if !seen.insert(path.clone()) {
                return Err(super::GatewayError::Archive(ArchiveError::DuplicatePath));
            }

            if entry_type == EntryType::Directory {
                if path != "payload" {
                    let payload = path
                        .strip_prefix("payload/")
                        .ok_or(super::GatewayError::Archive(ArchiveError::InvalidPath))?;
                    payload
                        .parse::<PayloadPath>()
                        .map_err(|_| super::GatewayError::Archive(ArchiveError::InvalidPath))?;
                    directories.insert(payload.to_owned());
                }
                continue;
            }
            if entry_type != EntryType::Regular {
                return Err(super::GatewayError::Archive(ArchiveError::InvalidEntryType));
            }
            if path == "request.json" {
                incoming
                    .write_request(&mut entry)
                    .map_err(|error| super::GatewayError::Queue(Box::new(error)))?;
                request_seen = true;
                continue;
            }
            let payload = path
                .strip_prefix("payload/")
                .ok_or(super::GatewayError::Archive(ArchiveError::InvalidPath))?;
            let payload = payload
                .parse::<PayloadPath>()
                .map_err(|_| super::GatewayError::Archive(ArchiveError::InvalidPath))?;
            incoming
                .add_payload(payload.clone(), &mut entry)
                .map_err(|error| super::GatewayError::Queue(Box::new(error)))?;
            payload_files.push(payload.as_str().to_owned());
        }
    }

    let mut limited = archive.into_inner();
    let mut trailing = [0_u8; TRAILING_BUFFER_BYTES];
    loop {
        let read = limited
            .read(&mut trailing)
            .map_err(ArchiveError::io)
            .map_err(super::GatewayError::Archive)?;
        if read == 0 {
            break;
        }
        if trailing[..read].iter().any(|byte| *byte != 0) {
            return Err(super::GatewayError::Archive(ArchiveError::TrailingData));
        }
    }
    if limited.limit() == 0 {
        return Err(super::GatewayError::Archive(
            ArchiveError::ArchiveTooLarge {
                maximum: maximum_archive_bytes,
            },
        ));
    }
    if !request_seen {
        return Err(super::GatewayError::Archive(ArchiveError::MissingRequest));
    }
    if directories.iter().any(|directory| {
        !payload_files
            .iter()
            .any(|file| file.starts_with(&format!("{directory}/")))
    }) {
        return Err(super::GatewayError::Archive(ArchiveError::EmptyDirectory));
    }

    let outcome = incoming
        .accept_for(client_id)
        .map_err(|error| super::GatewayError::Queue(Box::new(error)))?;
    Ok(SubmitResponse::new(map_outcome(outcome)))
}

fn maximum_archive_bytes(limits: PackageLimits) -> u64 {
    let entry_overhead = TAR_BLOCK_BYTES.saturating_add(MAXIMUM_PATH_EXTENSION_BYTES_PER_ENTRY);
    let entries = u64::try_from(limits.maximum_entry_count)
        .unwrap_or(u64::MAX)
        .saturating_add(4);
    limits
        .maximum_total_bytes
        .saturating_add(entry_overhead.saturating_mul(entries))
        .saturating_add(TAR_BLOCK_BYTES.saturating_mul(2))
}

fn normalize_entry_path(path: &str, entry_type: EntryType) -> Result<String, ArchiveError> {
    let normalized = if entry_type == EntryType::Directory {
        path.strip_suffix('/').unwrap_or(path)
    } else {
        path
    };
    if normalized.is_empty()
        || normalized.starts_with('/')
        || normalized.contains('\\')
        || normalized
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(ArchiveError::InvalidPath);
    }
    if normalized != "request.json"
        && normalized != "payload"
        && !normalized.starts_with("payload/")
    {
        return Err(ArchiveError::InvalidPath);
    }
    Ok(normalized.into())
}

fn map_outcome(outcome: EnqueueOutcome) -> SubmitOutcome {
    match outcome {
        EnqueueOutcome::Accepted { request_id, digest } => SubmitOutcome::Accepted {
            request_id,
            digest: digest.as_revision(),
        },
        EnqueueOutcome::Existing {
            request_id,
            digest,
            state,
        } => SubmitOutcome::Existing {
            request_id,
            digest: digest.as_revision(),
            state: map_state(state),
        },
    }
}

const fn map_state(state: QueueState) -> RequestState {
    match state {
        QueueState::Pending => RequestState::Pending,
        QueueState::Processing => RequestState::Processing,
        QueueState::Completed => RequestState::Completed,
        QueueState::Failed => RequestState::Failed,
    }
}

/// Invalid or unsupported uncompressed tar input.
#[derive(Debug)]
pub enum ArchiveError {
    /// The tar stream could not be read or decoded.
    Io(io::Error),
    /// The complete archive exceeded the protocol byte bound.
    ArchiveTooLarge {
        /// Maximum accepted raw archive bytes.
        maximum: u64,
    },
    /// The archive contained too many logical entries.
    TooManyEntries {
        /// Maximum accepted logical entries.
        maximum: usize,
    },
    /// An archive path was non-UTF-8, non-normalized, or outside the package.
    InvalidPath,
    /// Two archive entries resolved to the same normalized path.
    DuplicatePath,
    /// An entry was not a regular file or directory.
    InvalidEntryType,
    /// A sparse-file representation was rejected.
    SparseEntry,
    /// PAX extensions are outside the initial submit protocol.
    UnsupportedExtension,
    /// Nonzero bytes followed the end-of-archive marker.
    TrailingData,
    /// The archive omitted `request.json`.
    MissingRequest,
    /// An explicit payload directory did not contain a payload file.
    EmptyDirectory,
}

impl ArchiveError {
    fn io(error: io::Error) -> Self {
        Self::Io(error)
    }

    /// Returns the stable protocol classification for this failure.
    #[must_use]
    pub fn error_code(&self) -> ErrorCode {
        match self {
            Self::ArchiveTooLarge { .. } | Self::TooManyEntries { .. } => ErrorCode::LimitExceeded,
            Self::InvalidPath => ErrorCode::InvalidPath,
            Self::Io(error) if is_transport_error(error) => ErrorCode::TemporaryFailure,
            Self::Io(_)
            | Self::DuplicatePath
            | Self::InvalidEntryType
            | Self::SparseEntry
            | Self::UnsupportedExtension
            | Self::TrailingData
            | Self::MissingRequest
            | Self::EmptyDirectory => ErrorCode::InvalidRequest,
        }
    }
}

fn is_transport_error(error: &io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|source| source.downcast_ref::<TransportReadError>().is_some())
}

struct TransportReader<R>(R);

impl<R: Read> Read for TransportReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.0.read(buffer).map_err(|error| {
            let kind = error.kind();
            io::Error::new(kind, TransportReadError(error))
        })
    }
}

#[derive(Debug)]
struct TransportReadError(io::Error);

impl fmt::Display for TransportReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::error::Error for TransportReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

impl fmt::Display for ArchiveError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "tar input failed: {error}"),
            Self::ArchiveTooLarge { maximum } => {
                write!(formatter, "tar input exceeds {maximum} bytes")
            }
            Self::TooManyEntries { maximum } => {
                write!(formatter, "tar input exceeds {maximum} entries")
            }
            Self::InvalidPath => formatter.write_str("tar entry path is invalid"),
            Self::DuplicatePath => formatter.write_str("tar entry path is duplicated"),
            Self::InvalidEntryType => formatter.write_str("tar entry type is not allowed"),
            Self::SparseEntry => formatter.write_str("sparse tar entries are not allowed"),
            Self::UnsupportedExtension => {
                formatter.write_str("PAX tar extensions are not supported")
            }
            Self::TrailingData => formatter.write_str("tar input has nonzero trailing data"),
            Self::MissingRequest => formatter.write_str("tar input has no request.json"),
            Self::EmptyDirectory => formatter.write_str("tar input has an empty payload directory"),
        }
    }
}

impl std::error::Error for ArchiveError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

//! Restricted forced-command Gateway for authenticated coding agents.

mod config;
mod read;
mod submit;

use std::fmt;
use std::io::Read;

use agent_knowledge_core::{ErrorCode, PathAttestation, PathAttestationError};
use agent_knowledge_protocol::{
    ClientId, GetRequest, GetResponse, ListRequest, ListResponse, SearchRequest, SubmitResponse,
};
use agent_knowledge_queue::{FileQueue, PackagePolicy, QueueError};
use agent_knowledge_repository::{CommittedReadError, CommittedStore};

pub use config::{CURRENT_GATEWAY_CONFIG_VERSION, GatewayConfigError, GatewaySettings};
pub use read::ReadRequestError;
pub use submit::ArchiveError;

/// Opened Gateway dependencies for one forced-command process.
#[derive(Debug)]
pub struct Gateway {
    queue: FileQueue,
    committed: CommittedStore,
    settings: GatewaySettings,
}

impl Gateway {
    /// Opens the durable queue using validated deployment settings.
    ///
    /// # Errors
    ///
    /// Returns an error when the queue cannot be initialized or pinned.
    pub fn open(settings: &GatewaySettings) -> Result<Self, GatewayError> {
        let resolved = [
            PathAttestation::resolve_destination(settings.queue_root())
                .map_err(GatewayError::Attestation)?,
            PathAttestation::resolve_destination(settings.git_directory())
                .map_err(GatewayError::Attestation)?,
            PathAttestation::resolve_destination(settings.content_root())
                .map_err(GatewayError::Attestation)?,
        ];
        validate_disjoint_storage(&resolved)?;
        let committed = CommittedStore::open(
            settings.git_directory(),
            settings.content_root(),
            settings.official_branch(),
        )
        .map_err(|error| GatewayError::CommittedRead(Box::new(error)))?;
        let queue = FileQueue::initialize(settings.queue_root(), PackagePolicy::default())
            .map_err(|error| GatewayError::Queue(Box::new(error)))?;
        let [repository, content] = committed
            .storage_attestations()
            .map_err(GatewayError::Attestation)?;
        let queue_storage = queue
            .storage_attestation()
            .map_err(GatewayError::Attestation)?;
        validate_disjoint_storage(&[queue_storage, repository, content])?;
        Ok(Self {
            queue,
            committed,
            settings: settings.clone(),
        })
    }

    /// Streams and durably accepts one authenticated tar submission.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed archives, invalid packages, limits, or
    /// durable queue failures. No unchecked archive entry is extracted.
    pub fn submit(
        &self,
        client_id: ClientId,
        input: impl Read,
    ) -> Result<SubmitResponse, GatewayError> {
        submit::submit(&self.queue, client_id, input)
    }

    /// Lists matching committed documents in canonical path order.
    pub fn list(&self, request: &ListRequest) -> Result<ListResponse, GatewayError> {
        read::list(&self.settings, &self.committed, request, false)
    }

    /// Lists matching committed documents from most recently changed.
    pub fn recent(&self, request: &ListRequest) -> Result<ListResponse, GatewayError> {
        read::list(&self.settings, &self.committed, request, true)
    }

    /// Retrieves one exact committed Markdown document.
    pub fn get(&self, request: GetRequest) -> Result<GetResponse, GatewayError> {
        read::get(&self.settings, &self.committed, request)
    }

    /// Searches committed Markdown and configured metadata fields.
    pub fn search(&self, request: &SearchRequest) -> Result<ListResponse, GatewayError> {
        read::search(&self.settings, &self.committed, request)
    }
}

/// Failure while opening or serving one Gateway operation.
#[derive(Debug)]
pub enum GatewayError {
    /// Durable queue initialization or acceptance failed.
    Queue(Box<QueueError>),
    /// The untrusted tar stream violated the wire protocol.
    Archive(ArchiveError),
    /// A read request violated the bounded versioned protocol.
    ReadRequest(ReadRequestError),
    /// Opening or querying committed content failed.
    CommittedRead(Box<CommittedReadError>),
    /// Storage roots could not be safely resolved and attested.
    Attestation(PathAttestationError),
    /// Two configured storage roots resolve to overlapping locations.
    OverlappingStorage,
}

impl GatewayError {
    /// Returns the stable protocol classification for this failure.
    #[must_use]
    pub fn error_code(&self) -> ErrorCode {
        match self {
            Self::Queue(error) => error.error_code(),
            Self::Archive(error) => error.error_code(),
            Self::ReadRequest(error) => error.error_code(),
            Self::CommittedRead(error) => read::committed_error_code(error),
            Self::Attestation(_) | Self::OverlappingStorage => ErrorCode::InternalError,
        }
    }
}

impl fmt::Display for GatewayError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Queue(error) => write!(formatter, "Gateway queue operation failed: {error}"),
            Self::Archive(error) => write!(formatter, "invalid submit archive: {error}"),
            Self::ReadRequest(error) => write!(formatter, "invalid read request: {error}"),
            Self::CommittedRead(error) => write!(formatter, "committed read failed: {error}"),
            Self::Attestation(error) => {
                write!(formatter, "Gateway storage attestation failed: {error}")
            }
            Self::OverlappingStorage => {
                formatter.write_str("Gateway storage roots must not overlap")
            }
        }
    }
}

impl std::error::Error for GatewayError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Queue(error) => Some(error.as_ref()),
            Self::Archive(error) => Some(error),
            Self::ReadRequest(error) => Some(error),
            Self::CommittedRead(error) => Some(error.as_ref()),
            Self::Attestation(error) => Some(error),
            Self::OverlappingStorage => None,
        }
    }
}

fn validate_disjoint_storage(storage: &[PathAttestation]) -> Result<(), GatewayError> {
    for (index, first) in storage.iter().enumerate() {
        for second in &storage[index + 1..] {
            if first.is_within(second) || second.is_within(first) {
                return Err(GatewayError::OverlappingStorage);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;

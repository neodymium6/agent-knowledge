//! Restricted forced-command Gateway for authenticated coding agents.

mod config;
mod read;
mod status;
mod submit;

use std::fmt;
use std::io::Read;

use agent_knowledge_core::{ErrorCode, PathAttestation, PathAttestationError, RequestId};
use agent_knowledge_protocol::{
    ClientId, GetRequest, GetResponse, ListRequest, ListResponse, SearchRequest, StatusRequest,
    StatusResponse, SubmitResponse,
};
use agent_knowledge_queue::{FileQueue, PackagePolicy, QueueError, QueueReader};
use agent_knowledge_repository::{CommittedReadError, CommittedStore};

pub use config::{CURRENT_GATEWAY_CONFIG_VERSION, GatewayConfigError, GatewaySettings};
pub use read::ReadRequestError;
pub use submit::ArchiveError;

/// Gateway state opened exclusively for accepting submissions.
#[derive(Debug)]
pub struct SubmitGateway {
    queue: FileQueue,
}

impl SubmitGateway {
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
        let queue = FileQueue::initialize(resolved[0].stable_path(), PackagePolicy::default())
            .map_err(|error| GatewayError::Queue(Box::new(error)))?;
        let queue_storage = queue
            .storage_attestation()
            .map_err(GatewayError::Attestation)?;
        if !resolved[0].matches_destination(&queue_storage) {
            return Err(GatewayError::Attestation(
                PathAttestationError::BindingMismatch,
            ));
        }
        Ok(Self { queue })
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
}

/// Gateway state opened exclusively for durable request-status reads.
#[derive(Debug)]
pub struct StatusGateway {
    queue: QueueReader,
    settings: GatewaySettings,
}

impl StatusGateway {
    /// Opens an existing durable queue without initializing or locking it.
    ///
    /// The repository and content paths are intentionally not opened because
    /// request state is owned entirely by the durable queue.
    pub fn open_until(
        settings: &GatewaySettings,
        deadline: Option<std::time::Instant>,
    ) -> Result<Self, GatewayError> {
        let resolved = PathAttestation::resolve_destination(settings.queue_root())
            .map_err(GatewayError::Attestation)?;
        let queue = QueueReader::open_until(resolved.stable_path(), deadline)
            .map_err(|error| GatewayError::Queue(Box::new(error)))?;
        ensure_deadline(deadline)?;
        let queue_storage = queue
            .storage_attestation()
            .map_err(GatewayError::Attestation)?;
        if !resolved.matches_destination(&queue_storage) {
            return Err(GatewayError::Attestation(
                PathAttestationError::BindingMismatch,
            ));
        }
        ensure_deadline(deadline)?;
        Ok(Self {
            queue,
            settings: settings.clone(),
        })
    }

    /// Retrieves one durable request state under the configured read budget.
    pub fn status(&self, request: StatusRequest) -> Result<StatusResponse, GatewayError> {
        let deadline = read::read_deadline(&self.settings)?;
        status::status(&self.settings, &self.queue, request, deadline)
            .map(|prepared| prepared.response)
    }

    /// Encodes one durable request-state response under an absolute deadline.
    pub fn status_encoded_until(
        &self,
        request: StatusRequest,
        deadline: std::time::Instant,
    ) -> Result<Vec<u8>, GatewayError> {
        status::status(&self.settings, &self.queue, request, deadline)
            .map(|prepared| prepared.encoded)
    }
}

/// Gateway state opened exclusively for committed reads.
#[derive(Debug)]
pub struct ReadGateway {
    committed: CommittedStore,
    settings: GatewaySettings,
}

impl ReadGateway {
    /// Opens the committed store while applying an optional read deadline to
    /// repository inspection and initialization boundaries.
    pub fn open_until(
        settings: &GatewaySettings,
        deadline: Option<std::time::Instant>,
    ) -> Result<Self, GatewayError> {
        let resolved = [
            PathAttestation::resolve_destination(settings.git_directory())
                .map_err(GatewayError::Attestation)?,
            PathAttestation::resolve_destination(settings.content_root())
                .map_err(GatewayError::Attestation)?,
        ];
        validate_disjoint_storage(&resolved)?;
        let committed = CommittedStore::open_until(
            resolved[0].stable_path(),
            resolved[1].stable_path(),
            settings.official_branch(),
            deadline,
        )
        .map_err(|error| GatewayError::CommittedRead(Box::new(error)))?;
        ensure_deadline(deadline)?;
        let [repository, content] = committed
            .storage_attestations()
            .map_err(GatewayError::Attestation)?;
        if !resolved[0].matches_destination(&repository)
            || !resolved[1].matches_destination(&content)
        {
            return Err(GatewayError::Attestation(
                PathAttestationError::BindingMismatch,
            ));
        }
        ensure_deadline(deadline)?;
        Ok(Self {
            committed,
            settings: settings.clone(),
        })
    }

    /// Lists matching committed documents in canonical path order.
    pub fn list(&self, request: &ListRequest) -> Result<ListResponse, GatewayError> {
        let deadline = read::read_deadline(&self.settings)?;
        read::list(&self.settings, &self.committed, request, false, deadline)
            .map(|prepared| prepared.response)
    }

    /// Lists matching committed documents from most recently changed.
    pub fn recent(&self, request: &ListRequest) -> Result<ListResponse, GatewayError> {
        let deadline = read::read_deadline(&self.settings)?;
        read::list(&self.settings, &self.committed, request, true, deadline)
            .map(|prepared| prepared.response)
    }

    /// Retrieves one exact committed Markdown document.
    pub fn get(&self, request: GetRequest) -> Result<GetResponse, GatewayError> {
        read::get(&self.settings, &self.committed, request).map(|prepared| prepared.response)
    }

    /// Searches committed Markdown and configured metadata fields.
    pub fn search(&self, request: &SearchRequest) -> Result<ListResponse, GatewayError> {
        read::search(&self.settings, &self.committed, request).map(|prepared| prepared.response)
    }

    /// Encodes one list response exactly once under the supplied deadline.
    pub fn list_encoded_until(
        &self,
        request: &ListRequest,
        recent: bool,
        deadline: std::time::Instant,
    ) -> Result<Vec<u8>, GatewayError> {
        read::list(&self.settings, &self.committed, request, recent, deadline)
            .map(|prepared| prepared.encoded)
    }

    /// Encodes one exact-document response once under the supplied deadline.
    pub fn get_encoded_until(
        &self,
        request: GetRequest,
        deadline: std::time::Instant,
    ) -> Result<Vec<u8>, GatewayError> {
        read::get_until(&self.settings, &self.committed, request, deadline)
            .map(|prepared| prepared.encoded)
    }

    /// Encodes one search response exactly once under the supplied deadline.
    pub fn search_encoded_until(
        &self,
        request: &SearchRequest,
        deadline: std::time::Instant,
    ) -> Result<Vec<u8>, GatewayError> {
        read::search_until(&self.settings, &self.committed, request, deadline)
            .map(|prepared| prepared.encoded)
    }
}

fn ensure_deadline(deadline: Option<std::time::Instant>) -> Result<(), GatewayError> {
    if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
        Err(GatewayError::OperationDeadlineExceeded)
    } else {
        Ok(())
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
    /// A bounded Gateway operation exceeded its absolute deadline.
    OperationDeadlineExceeded,
    /// No durable state exists for the requested change request.
    RequestNotFound {
        /// Requested immutable change-request identifier.
        request_id: RequestId,
    },
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
            Self::OperationDeadlineExceeded => ErrorCode::TemporaryFailure,
            Self::RequestNotFound { .. } => ErrorCode::RequestNotFound,
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
            Self::OperationDeadlineExceeded => {
                formatter.write_str("Gateway operation deadline expired")
            }
            Self::RequestNotFound { request_id } => {
                write!(
                    formatter,
                    "request {request_id} does not exist in durable queue state"
                )
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
            Self::OverlappingStorage
            | Self::OperationDeadlineExceeded
            | Self::RequestNotFound { .. } => None,
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

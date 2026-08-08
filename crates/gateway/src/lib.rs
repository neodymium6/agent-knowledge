//! Restricted forced-command Gateway for authenticated coding agents.

mod config;
mod ingress;
mod read;
mod submit;

use agent_knowledge_core::{ErrorCode, PathAttestation, PathAttestationError, RequestId};
use agent_knowledge_protocol::{
    ExportRequest, GetRequest, GetResponse, ListRequest, ListResponse, SearchRequest,
};
use agent_knowledge_queue::QueueError;
use agent_knowledge_repository::{CommittedReadError, CommittedStore};
use std::fmt;

pub use config::{CURRENT_GATEWAY_CONFIG_VERSION, GatewayConfigError, GatewaySettings};
pub use ingress::{
    IngressClient, IngressClientError, IngressServeError, serve as serve_ingress,
    serve_until as serve_ingress_until,
};
pub use read::{PreparedExport, ReadRequestError};
pub use submit::ArchiveError;

#[cfg(test)]
#[derive(Debug)]
struct SubmitGateway {
    queue: agent_knowledge_queue::FileQueue,
}

#[cfg(test)]
impl SubmitGateway {
    fn open(queue_root: &std::path::Path) -> Result<Self, GatewayError> {
        let queue = agent_knowledge_queue::FileQueue::initialize(
            queue_root,
            agent_knowledge_queue::PackagePolicy::default(),
        )
        .map_err(|error| GatewayError::Queue(Box::new(error)))?;
        Ok(Self { queue })
    }

    fn submit(
        &self,
        client_id: agent_knowledge_protocol::ClientId,
        input: impl std::io::Read,
    ) -> Result<agent_knowledge_protocol::SubmitResponse, GatewayError> {
        submit::submit_until(&self.queue, client_id, input, None)
    }
}

#[cfg(test)]
#[derive(Debug)]
struct StatusGateway {
    queue: agent_knowledge_queue::QueueReader,
}

#[cfg(test)]
impl StatusGateway {
    fn open(queue_root: &std::path::Path) -> Result<Self, GatewayError> {
        let queue = agent_knowledge_queue::QueueReader::open_until(queue_root, None)
            .map_err(|error| GatewayError::Queue(Box::new(error)))?;
        Ok(Self { queue })
    }

    fn status(
        &self,
        request: agent_knowledge_protocol::StatusRequest,
    ) -> Result<agent_knowledge_protocol::StatusResponse, GatewayError> {
        let request_id = request.request_id;
        let observed = self
            .queue
            .status_until(request_id, None)
            .map_err(|error| GatewayError::Queue(Box::new(error)))?
            .ok_or(GatewayError::RequestNotFound { request_id })?;
        let status = match observed {
            agent_knowledge_queue::QueueRequestStatus::Pending => {
                agent_knowledge_protocol::RequestStatus::Pending
            }
            agent_knowledge_queue::QueueRequestStatus::Processing => {
                agent_knowledge_protocol::RequestStatus::Processing
            }
            agent_knowledge_queue::QueueRequestStatus::Completed => {
                agent_knowledge_protocol::RequestStatus::Completed
            }
            agent_knowledge_queue::QueueRequestStatus::Failed {
                error_code,
                failed_at,
            } => agent_knowledge_protocol::RequestStatus::Failed {
                error_code,
                failed_at,
            },
        };
        Ok(agent_knowledge_protocol::StatusResponse::new(
            request_id, status,
        ))
    }
}

/// Gateway state opened exclusively for committed reads.
#[derive(Debug)]
pub struct ReadGateway {
    committed: CommittedStore,
    search_indexes: Option<PathAttestation>,
    settings: GatewaySettings,
}

impl ReadGateway {
    /// Opens the committed store while applying an optional read deadline to
    /// repository inspection and initialization boundaries.
    pub fn open_until(
        settings: &GatewaySettings,
        deadline: Option<std::time::Instant>,
    ) -> Result<Self, GatewayError> {
        let mut resolved = vec![
            PathAttestation::resolve_destination(settings.git_directory())
                .map_err(GatewayError::Attestation)?,
            PathAttestation::resolve_destination(settings.content_root())
                .map_err(GatewayError::Attestation)?,
        ];
        if let Some(search_index_root) = settings.search_index_root() {
            resolved.push(
                PathAttestation::resolve_destination(search_index_root)
                    .map_err(GatewayError::Attestation)?,
            );
        }
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
        let search_indexes = (resolved.len() == 3).then(|| resolved.remove(2));
        Ok(Self {
            committed,
            search_indexes,
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

    /// Encodes one committed document bundle as a deterministic tar archive.
    pub fn prepare_export_until(
        &self,
        request: ExportRequest,
        deadline: std::time::Instant,
    ) -> Result<PreparedExport, GatewayError> {
        read::export_until(&self.settings, &self.committed, request, deadline)
    }

    /// Searches committed Markdown and configured metadata fields.
    pub fn search(&self, request: &SearchRequest) -> Result<ListResponse, GatewayError> {
        read::search(
            &self.settings,
            &self.committed,
            self.search_indexes
                .as_ref()
                .map(PathAttestation::stable_path),
            request,
        )
        .map(|prepared| prepared.response)
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
        read::search_until(
            &self.settings,
            &self.committed,
            self.search_indexes
                .as_ref()
                .map(PathAttestation::stable_path),
            request,
            deadline,
        )
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
    /// Configured derived search state was absent, stale, or unreadable.
    SearchIndexUnavailable,
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
            Self::OperationDeadlineExceeded | Self::SearchIndexUnavailable => {
                ErrorCode::TemporaryFailure
            }
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
            Self::SearchIndexUnavailable => {
                formatter.write_str("configured search index is temporarily unavailable")
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
            | Self::SearchIndexUnavailable
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

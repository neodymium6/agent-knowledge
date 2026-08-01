use std::fmt;
use std::io::{self, Write};
use std::time::Instant;

use agent_knowledge_core::{DocumentLimits, ErrorCode};
use agent_knowledge_protocol::{
    CURRENT_GATEWAY_PROTOCOL_VERSION, DocumentContent, DocumentSummary, GetRequest, GetResponse,
    ListRequest, ListResponse, ReadFilterRequest, SearchRequest,
};
use agent_knowledge_queue::PackagePolicy;
use agent_knowledge_repository::{
    CommittedReadError, CommittedStore, ContentPolicy, DocumentRecord, LinearSearch, ReadFilter,
    SearchBackend, SearchMetadataFields, SearchPolicy,
};

use crate::{GatewayError, GatewaySettings};

pub(super) fn list(
    settings: &GatewaySettings,
    store: &CommittedStore,
    request: &ListRequest,
    recent: bool,
    deadline: Instant,
) -> Result<PreparedResponse<ListResponse>, GatewayError> {
    validate_version(request.protocol_version)?;
    validate_filter(&request.filter)?;
    validate_result_limit(settings, request.maximum_results)?;
    let snapshot = snapshot(settings, store, deadline)?;
    let filter = repository_filter(&request.filter);
    let records = if recent {
        snapshot.recent(&filter, request.maximum_results)
    } else {
        snapshot.list(&filter, request.maximum_results)
    }
    .map_err(committed)?;
    let documents = records
        .into_iter()
        .map(document_summary)
        .collect::<Result<Vec<_>, _>>()?;
    let response = ListResponse::new(snapshot.commit().to_owned(), documents);
    drop(snapshot);
    prepare_response(settings, response, deadline)
}

pub(super) fn get(
    settings: &GatewaySettings,
    store: &CommittedStore,
    request: GetRequest,
) -> Result<PreparedResponse<GetResponse>, GatewayError> {
    get_until(settings, store, request, read_deadline(settings)?)
}

pub(super) fn get_until(
    settings: &GatewaySettings,
    store: &CommittedStore,
    request: GetRequest,
    deadline: Instant,
) -> Result<PreparedResponse<GetResponse>, GatewayError> {
    validate_version(request.protocol_version)?;
    let snapshot = snapshot(settings, store, deadline)?;
    let committed_document = snapshot.get(request.document_id).map_err(committed)?;
    let summary = document_summary(committed_document.record())?;
    let markdown = std::str::from_utf8(committed_document.markdown())
        .map_err(|_| {
            committed(CommittedReadError::InvalidMarkdownEncoding {
                document_id: request.document_id,
            })
        })?
        .to_owned();
    let response = GetResponse::new(
        snapshot.commit().to_owned(),
        DocumentContent { summary, markdown },
    );
    drop(snapshot);
    prepare_response(settings, response, deadline)
}

pub(super) fn search(
    settings: &GatewaySettings,
    store: &CommittedStore,
    request: &SearchRequest,
) -> Result<PreparedResponse<ListResponse>, GatewayError> {
    search_until(settings, store, request, read_deadline(settings)?)
}

pub(super) fn search_until(
    settings: &GatewaySettings,
    store: &CommittedStore,
    request: &SearchRequest,
    deadline: Instant,
) -> Result<PreparedResponse<ListResponse>, GatewayError> {
    validate_version(request.protocol_version)?;
    validate_filter(&request.filter)?;
    validate_result_limit(settings, request.maximum_results)?;
    let snapshot = snapshot(settings, store, deadline)?;
    let fields = settings.search_metadata_fields();
    let search = LinearSearch::new(SearchMetadataFields::new(
        fields[0], fields[1], fields[2], fields[3],
    ));
    let records = search
        .search(
            &snapshot,
            &request.query,
            &repository_filter(&request.filter),
            SearchPolicy {
                maximum_query_characters: settings.maximum_search_query_characters(),
                maximum_results: request.maximum_results,
                maximum_scanned_documents: settings.maximum_search_documents(),
                maximum_scanned_markdown_bytes: settings.maximum_search_markdown_bytes(),
                deadline: Some(deadline),
            },
        )
        .map_err(committed)?;
    let documents = records
        .into_iter()
        .map(document_summary)
        .collect::<Result<Vec<_>, _>>()?;
    let response = ListResponse::new(snapshot.commit().to_owned(), documents);
    drop(snapshot);
    prepare_response(settings, response, deadline)
}

fn snapshot(
    settings: &GatewaySettings,
    store: &CommittedStore,
    deadline: Instant,
) -> Result<agent_knowledge_repository::CommittedSnapshot, GatewayError> {
    let policy = ContentPolicy {
        maximum_entry_count: settings.maximum_index_entries(),
        maximum_total_markdown_bytes: settings.maximum_index_markdown_bytes(),
        scan_deadline: Some(deadline),
        ..ContentPolicy::default()
    };
    store
        .snapshot(policy, &PackagePolicy::default())
        .map_err(committed)
}

pub(super) fn read_deadline(settings: &GatewaySettings) -> Result<Instant, GatewayError> {
    Instant::now()
        .checked_add(settings.read_operation_timeout())
        .ok_or(GatewayError::ReadRequest(ReadRequestError::InvalidDeadline))
}

fn prepare_response<T>(
    settings: &GatewaySettings,
    response: T,
    deadline: Instant,
) -> Result<PreparedResponse<T>, GatewayError>
where
    T: serde::Serialize,
{
    let mut buffer = ResponseBuffer {
        bytes: Vec::new(),
        maximum: settings.maximum_response_bytes(),
        deadline: Some(deadline),
        deadline_exceeded: false,
        limit_exceeded: false,
    };
    let encoded = serde_json::to_writer(&mut buffer, &response)
        .and_then(|()| buffer.write_all(b"\n").map_err(serde_json::Error::io));
    if encoded.is_err() {
        if buffer.deadline_exceeded {
            return Err(committed(CommittedReadError::OperationDeadlineExceeded));
        }
        if buffer.limit_exceeded {
            return Err(GatewayError::ReadRequest(
                ReadRequestError::ResponseTooLarge {
                    maximum: settings.maximum_response_bytes(),
                },
            ));
        }
        return Err(GatewayError::ReadRequest(
            ReadRequestError::ResponseEncoding,
        ));
    }
    if Instant::now() >= deadline {
        return Err(committed(CommittedReadError::OperationDeadlineExceeded));
    }
    Ok(PreparedResponse {
        response,
        encoded: buffer.bytes,
    })
}

pub(super) struct PreparedResponse<T> {
    pub(super) response: T,
    pub(super) encoded: Vec<u8>,
}

struct ResponseBuffer {
    bytes: Vec<u8>,
    maximum: u64,
    deadline: Option<Instant>,
    deadline_exceeded: bool,
    limit_exceeded: bool,
}

impl Write for ResponseBuffer {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            self.deadline_exceeded = true;
            return Err(io::Error::other("response deadline exceeded"));
        }
        let next = (self.bytes.len() as u64)
            .checked_add(buffer.len() as u64)
            .ok_or_else(|| io::Error::other("response byte limit exceeded"))?;
        if next > self.maximum {
            self.limit_exceeded = true;
            return Err(io::Error::other("response byte limit exceeded"));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn repository_filter(filter: &ReadFilterRequest) -> ReadFilter {
    ReadFilter::new(
        filter.project.clone(),
        filter.tag.clone(),
        filter.session,
        filter.include_archived,
    )
}

fn document_summary(record: &DocumentRecord) -> Result<DocumentSummary, GatewayError> {
    let path = record
        .relative_path()
        .to_str()
        .ok_or(GatewayError::ReadRequest(
            ReadRequestError::InvalidCommittedPath,
        ))?
        .to_owned();
    Ok(DocumentSummary {
        path,
        document_type: record.location().document_type(),
        project: record.location().project().cloned(),
        archived: record.location().is_archived(),
        revision: record.revision(),
        metadata: record.metadata().clone(),
    })
}

fn validate_version(version: u16) -> Result<(), GatewayError> {
    if version != CURRENT_GATEWAY_PROTOCOL_VERSION {
        return Err(GatewayError::ReadRequest(
            ReadRequestError::UnsupportedProtocolVersion { found: version },
        ));
    }
    Ok(())
}

fn validate_filter(filter: &ReadFilterRequest) -> Result<(), GatewayError> {
    if let Some(tag) = &filter.tag {
        let limits = DocumentLimits::default();
        if tag.trim().is_empty() || tag.chars().any(char::is_control) {
            return Err(GatewayError::ReadRequest(ReadRequestError::InvalidTag));
        }
        let actual = tag.chars().count();
        if actual > limits.maximum_tag_characters {
            return Err(GatewayError::ReadRequest(ReadRequestError::TagTooLong {
                maximum: limits.maximum_tag_characters,
                actual,
            }));
        }
    }
    Ok(())
}

fn validate_result_limit(settings: &GatewaySettings, maximum: usize) -> Result<(), GatewayError> {
    if maximum == 0 || maximum > settings.maximum_read_results() {
        return Err(GatewayError::ReadRequest(
            ReadRequestError::InvalidResultLimit {
                maximum: settings.maximum_read_results(),
                actual: maximum,
            },
        ));
    }
    Ok(())
}

fn committed(error: CommittedReadError) -> GatewayError {
    GatewayError::CommittedRead(Box::new(error))
}

pub(super) fn committed_error_code(error: &CommittedReadError) -> ErrorCode {
    match error {
        CommittedReadError::DocumentNotFound { .. } => ErrorCode::DocumentNotFound,
        CommittedReadError::EmptyQuery | CommittedReadError::InvalidResultLimit => {
            ErrorCode::InvalidRequest
        }
        CommittedReadError::QueryTooLong { .. }
        | CommittedReadError::SearchDocumentLimitExceeded { .. }
        | CommittedReadError::SearchMarkdownByteLimitExceeded { .. } => ErrorCode::LimitExceeded,
        CommittedReadError::Content(source)
            if matches!(
                source.as_ref(),
                agent_knowledge_repository::ContentIndexError::EntryLimitExceeded { .. }
                    | agent_knowledge_repository::ContentIndexError::MarkdownByteLimitExceeded { .. }
            ) =>
        {
            ErrorCode::LimitExceeded
        }
        CommittedReadError::Content(source)
            if matches!(
                source.as_ref(),
                agent_knowledge_repository::ContentIndexError::ScanDeadlineExceeded
                    | agent_knowledge_repository::ContentIndexError::FileChangedDuringScan(_)
            ) =>
        {
            ErrorCode::TemporaryFailure
        }
        CommittedReadError::Content(_) | CommittedReadError::InvalidMarkdownEncoding { .. } => {
            ErrorCode::ContentValidationFailed
        }
        CommittedReadError::Repository(_)
        | CommittedReadError::Io(_)
        | CommittedReadError::OverlappingPaths
        | CommittedReadError::InvalidOfficialBranch
        | CommittedReadError::StorageReplaced
        | CommittedReadError::Busy
        | CommittedReadError::CanonicalOutOfDate { .. }
        | CommittedReadError::PinnedPath(_)
        | CommittedReadError::ContentChanged { .. }
        | CommittedReadError::OperationDeadlineExceeded => ErrorCode::TemporaryFailure,
    }
}

/// A deterministic violation of the bounded committed-read request protocol.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadRequestError {
    /// The request named an unsupported Gateway protocol version.
    UnsupportedProtocolVersion {
        /// Version found in the request.
        found: u16,
    },
    /// An exact tag filter was empty or contained control characters.
    InvalidTag,
    /// An exact tag filter exceeded the document metadata bound.
    TagTooLong {
        /// Maximum accepted Unicode scalar values.
        maximum: usize,
        /// Supplied Unicode scalar values.
        actual: usize,
    },
    /// The requested result count was zero or exceeded deployment policy.
    InvalidResultLimit {
        /// Maximum configured results.
        maximum: usize,
        /// Requested results.
        actual: usize,
    },
    /// Validated committed content unexpectedly had a non-UTF-8 path.
    InvalidCommittedPath,
    /// The absolute operation deadline could not be represented.
    InvalidDeadline,
    /// The encoded successful response exceeded deployment policy.
    ResponseTooLarge {
        /// Maximum encoded response bytes.
        maximum: u64,
    },
    /// A typed successful response unexpectedly failed JSON encoding.
    ResponseEncoding,
}

impl ReadRequestError {
    #[must_use]
    pub const fn error_code(self) -> ErrorCode {
        match self {
            Self::UnsupportedProtocolVersion { .. } => ErrorCode::InvalidProtocol,
            Self::TagTooLong { .. } | Self::InvalidResultLimit { .. } => ErrorCode::LimitExceeded,
            Self::InvalidTag => ErrorCode::InvalidRequest,
            Self::InvalidCommittedPath => ErrorCode::ContentValidationFailed,
            Self::InvalidDeadline => ErrorCode::InternalError,
            Self::ResponseTooLarge { .. } => ErrorCode::LimitExceeded,
            Self::ResponseEncoding => ErrorCode::InternalError,
        }
    }
}

impl fmt::Display for ReadRequestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedProtocolVersion { found } => {
                write!(formatter, "unsupported Gateway protocol version {found}")
            }
            Self::InvalidTag => formatter.write_str("tag filter must be nonempty visible text"),
            Self::TagTooLong { maximum, actual } => {
                write!(
                    formatter,
                    "tag filter has {actual} characters; maximum is {maximum}"
                )
            }
            Self::InvalidResultLimit { maximum, actual } => write!(
                formatter,
                "maximum results is {actual}; configured maximum is {maximum}"
            ),
            Self::InvalidCommittedPath => {
                formatter.write_str("committed document path is not UTF-8")
            }
            Self::InvalidDeadline => formatter.write_str("read operation deadline is invalid"),
            Self::ResponseTooLarge { maximum } => {
                write!(formatter, "encoded response exceeds {maximum} bytes")
            }
            Self::ResponseEncoding => formatter.write_str("successful response encoding failed"),
        }
    }
}

impl std::error::Error for ReadRequestError {}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::path::PathBuf;

    use agent_knowledge_core::ErrorCode;
    use agent_knowledge_protocol::ListResponse;
    use agent_knowledge_repository::{CommittedReadError, ContentIndexError};

    use super::{ReadRequestError, ResponseBuffer, committed_error_code, prepare_response};
    use crate::{GatewayError, GatewaySettings};

    #[test]
    fn rejects_a_success_response_over_the_shared_wire_budget() {
        let settings = GatewaySettings::decode(
            "schema_version: 2\nstorage:\n  queue_root: /srv/fictional-knowledge/queue\n  git_directory: /srv/fictional-knowledge/repository\n  content_root: /srv/fictional-knowledge/content\nrepository:\n  official_branch: main\nreads:\n  maximum_results: 100\n  maximum_query_characters: 512\n  maximum_index_entries: 100000\n  maximum_index_markdown_bytes: 536870912\n  maximum_search_documents: 10000\n  maximum_search_markdown_bytes: 536870912\n  operation_timeout_seconds: 30\n  maximum_response_bytes: 8\n  search_metadata:\n    node: true\n    agent: true\n    session: true\n    request_id: true\ntransport:\n  submit_timeout_seconds: 300\n",
        )
        .unwrap_or_else(|error| panic!("response-budget settings must decode: {error}"));
        let response = ListResponse::new(
            "0123456789abcdef0123456789abcdef01234567".into(),
            Vec::new(),
        );
        assert!(matches!(
            prepare_response(
                &settings,
                response,
                std::time::Instant::now() + std::time::Duration::from_secs(1),
            ),
            Err(GatewayError::ReadRequest(
                ReadRequestError::ResponseTooLarge { maximum: 8 }
            ))
        ));
    }

    #[test]
    fn response_counter_reserves_the_newline_framing_byte() {
        let mut counter = ResponseBuffer {
            bytes: vec![b'1'],
            maximum: 8,
            deadline: None,
            deadline_exceeded: false,
            limit_exceeded: false,
        };
        counter
            .write_all(b"1234567")
            .unwrap_or_else(|error| panic!("seven JSON bytes should fit: {error}"));
        assert!(counter.write_all(b"8").is_err());
    }

    #[test]
    fn classifies_a_file_changed_during_scan_as_retryable() {
        let error = CommittedReadError::Content(Box::new(
            ContentIndexError::FileChangedDuringScan(PathBuf::from("fictional.md")),
        ));
        assert_eq!(committed_error_code(&error), ErrorCode::TemporaryFailure);
    }
}

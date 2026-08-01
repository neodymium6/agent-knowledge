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
) -> Result<ListResponse, GatewayError> {
    validate_version(request.protocol_version)?;
    validate_filter(&request.filter)?;
    validate_result_limit(settings, request.maximum_results)?;
    let deadline = read_deadline(settings)?;
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
    ensure_response_size(settings, &response, Some(deadline))?;
    Ok(response)
}

pub(super) fn get(
    settings: &GatewaySettings,
    store: &CommittedStore,
    request: GetRequest,
) -> Result<GetResponse, GatewayError> {
    validate_version(request.protocol_version)?;
    let deadline = read_deadline(settings)?;
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
    ensure_response_size(settings, &response, Some(deadline))?;
    Ok(response)
}

pub(super) fn search(
    settings: &GatewaySettings,
    store: &CommittedStore,
    request: &SearchRequest,
) -> Result<ListResponse, GatewayError> {
    validate_version(request.protocol_version)?;
    validate_filter(&request.filter)?;
    validate_result_limit(settings, request.maximum_results)?;
    let deadline = read_deadline(settings)?;
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
    ensure_response_size(settings, &response, Some(deadline))?;
    Ok(response)
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

fn read_deadline(settings: &GatewaySettings) -> Result<Instant, GatewayError> {
    Instant::now()
        .checked_add(settings.read_operation_timeout())
        .ok_or(GatewayError::ReadRequest(ReadRequestError::InvalidDeadline))
}

fn ensure_response_size(
    settings: &GatewaySettings,
    response: &impl serde::Serialize,
    deadline: Option<Instant>,
) -> Result<(), GatewayError> {
    let mut counter = ResponseCounter {
        // Successful control responses are newline-delimited JSON. Reserve the
        // framing byte so server and client enforce the same wire-byte limit.
        written: 1,
        maximum: settings.maximum_response_bytes(),
        deadline,
        deadline_exceeded: false,
    };
    if serde_json::to_writer(&mut counter, response).is_err() {
        if counter.deadline_exceeded {
            return Err(committed(CommittedReadError::OperationDeadlineExceeded));
        }
        return Err(GatewayError::ReadRequest(
            ReadRequestError::ResponseTooLarge {
                maximum: settings.maximum_response_bytes(),
            },
        ));
    }
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        return Err(committed(CommittedReadError::OperationDeadlineExceeded));
    }
    Ok(())
}

struct ResponseCounter {
    written: u64,
    maximum: u64,
    deadline: Option<Instant>,
    deadline_exceeded: bool,
}

impl Write for ResponseCounter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            self.deadline_exceeded = true;
            return Err(io::Error::other("response deadline exceeded"));
        }
        let next = self
            .written
            .checked_add(buffer.len() as u64)
            .ok_or_else(|| io::Error::other("response byte limit exceeded"))?;
        if next > self.maximum {
            return Err(io::Error::other("response byte limit exceeded"));
        }
        self.written = next;
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
        }
    }
}

impl std::error::Error for ReadRequestError {}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use agent_knowledge_protocol::ListResponse;

    use super::{ReadRequestError, ResponseCounter, ensure_response_size};
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
            ensure_response_size(&settings, &response, None),
            Err(GatewayError::ReadRequest(
                ReadRequestError::ResponseTooLarge { maximum: 8 }
            ))
        ));
    }

    #[test]
    fn response_counter_reserves_the_newline_framing_byte() {
        let mut counter = ResponseCounter {
            written: 1,
            maximum: 8,
            deadline: None,
            deadline_exceeded: false,
        };
        counter
            .write_all(b"1234567")
            .unwrap_or_else(|error| panic!("seven JSON bytes should fit: {error}"));
        assert!(counter.write_all(b"8").is_err());
    }
}

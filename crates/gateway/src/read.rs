use std::fmt;

use agent_knowledge_core::{DocumentLimits, ErrorCode};
use agent_knowledge_protocol::{
    CURRENT_GATEWAY_PROTOCOL_VERSION, DocumentContent, DocumentSummary, GetRequest, GetResponse,
    ListRequest, ListResponse, ReadFilterRequest, SearchRequest,
};
use agent_knowledge_queue::PackagePolicy;
use agent_knowledge_repository::{
    CommittedReadError, CommittedStore, ContentPolicy, DocumentRecord, LinearSearch, ReadFilter,
    SearchBackend, SearchMetadataFields,
};

use crate::{GatewayError, GatewaySettings};

pub(super) fn list(
    settings: &GatewaySettings,
    request: &ListRequest,
    recent: bool,
) -> Result<ListResponse, GatewayError> {
    validate_version(request.protocol_version)?;
    validate_filter(&request.filter)?;
    validate_result_limit(settings, request.maximum_results)?;
    let snapshot = snapshot(settings)?;
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
    Ok(ListResponse::new(snapshot.commit().to_owned(), documents))
}

pub(super) fn get(
    settings: &GatewaySettings,
    request: GetRequest,
) -> Result<GetResponse, GatewayError> {
    validate_version(request.protocol_version)?;
    let snapshot = snapshot(settings)?;
    let committed_document = snapshot.get(request.document_id).map_err(committed)?;
    let summary = document_summary(committed_document.record())?;
    let markdown = std::str::from_utf8(committed_document.markdown())
        .map_err(|_| {
            committed(CommittedReadError::InvalidMarkdownEncoding {
                document_id: request.document_id,
            })
        })?
        .to_owned();
    Ok(GetResponse::new(
        snapshot.commit().to_owned(),
        DocumentContent { summary, markdown },
    ))
}

pub(super) fn search(
    settings: &GatewaySettings,
    request: &SearchRequest,
) -> Result<ListResponse, GatewayError> {
    validate_version(request.protocol_version)?;
    validate_filter(&request.filter)?;
    validate_result_limit(settings, request.maximum_results)?;
    let snapshot = snapshot(settings)?;
    let fields = settings.search_metadata_fields();
    let search = LinearSearch::new(SearchMetadataFields::new(
        fields[0], fields[1], fields[2], fields[3],
    ));
    let records = search
        .search(
            &snapshot,
            &request.query,
            &repository_filter(&request.filter),
            settings.maximum_search_query_characters(),
            request.maximum_results,
        )
        .map_err(committed)?;
    let documents = records
        .into_iter()
        .map(document_summary)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ListResponse::new(snapshot.commit().to_owned(), documents))
}

fn snapshot(
    settings: &GatewaySettings,
) -> Result<agent_knowledge_repository::CommittedSnapshot, GatewayError> {
    let store = CommittedStore::open(
        settings.git_directory(),
        settings.content_root(),
        settings.official_branch(),
    )
    .map_err(committed)?;
    store
        .snapshot(ContentPolicy::default(), &PackagePolicy::default())
        .map_err(committed)
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
        CommittedReadError::QueryTooLong { .. } => ErrorCode::LimitExceeded,
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
        | CommittedReadError::ContentChanged { .. } => ErrorCode::TemporaryFailure,
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
}

impl ReadRequestError {
    #[must_use]
    pub const fn error_code(self) -> ErrorCode {
        match self {
            Self::UnsupportedProtocolVersion { .. } => ErrorCode::InvalidProtocol,
            Self::TagTooLong { .. } | Self::InvalidResultLimit { .. } => ErrorCode::LimitExceeded,
            Self::InvalidTag => ErrorCode::InvalidRequest,
            Self::InvalidCommittedPath => ErrorCode::ContentValidationFailed,
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
        }
    }
}

impl std::error::Error for ReadRequestError {}

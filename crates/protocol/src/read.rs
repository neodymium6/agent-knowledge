use agent_knowledge_core::{
    DocumentId, DocumentMetadata, DocumentType, ProjectId, Revision, SessionId,
};
use serde::{Deserialize, Serialize};

use crate::CURRENT_GATEWAY_PROTOCOL_VERSION;

/// Optional exact-match filters shared by committed list and search requests.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReadFilterRequest {
    /// Restricts results to one project.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<ProjectId>,
    /// Restricts results to one exact tag.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    /// Restricts results to one coding-agent session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<SessionId>,
    /// Includes documents below archive directories.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub include_archived: bool,
}

/// Input for committed `list` and `recent` operations.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ListRequest {
    /// Independent Gateway protocol version.
    pub protocol_version: u16,
    /// Exact-match result filters.
    #[serde(flatten)]
    pub filter: ReadFilterRequest,
    /// Maximum documents returned by this request.
    pub maximum_results: usize,
}

impl ListRequest {
    /// Constructs a request using the current protocol version.
    #[must_use]
    pub const fn new(filter: ReadFilterRequest, maximum_results: usize) -> Self {
        Self {
            protocol_version: CURRENT_GATEWAY_PROTOCOL_VERSION,
            filter,
            maximum_results,
        }
    }
}

/// Input for one committed document lookup.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GetRequest {
    /// Independent Gateway protocol version.
    pub protocol_version: u16,
    /// Permanent identity of the requested document.
    pub document_id: DocumentId,
}

impl GetRequest {
    /// Constructs a request using the current protocol version.
    #[must_use]
    pub const fn new(document_id: DocumentId) -> Self {
        Self {
            protocol_version: CURRENT_GATEWAY_PROTOCOL_VERSION,
            document_id,
        }
    }
}

/// Input for exporting one committed document bundle.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExportRequest {
    /// Independent Gateway protocol version.
    pub protocol_version: u16,
    /// Permanent identity of the document whose bundle is requested.
    pub document_id: DocumentId,
}

impl ExportRequest {
    /// Constructs a request using the current protocol version.
    #[must_use]
    pub const fn new(document_id: DocumentId) -> Self {
        Self {
            protocol_version: CURRENT_GATEWAY_PROTOCOL_VERSION,
            document_id,
        }
    }
}

/// Input for one committed full-text search.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SearchRequest {
    /// Independent Gateway protocol version.
    pub protocol_version: u16,
    /// Case-insensitive text query.
    pub query: String,
    /// Exact-match result filters.
    #[serde(flatten)]
    pub filter: ReadFilterRequest,
    /// Maximum documents returned by this request.
    pub maximum_results: usize,
}

impl SearchRequest {
    /// Constructs a request using the current protocol version.
    #[must_use]
    pub const fn new(query: String, filter: ReadFilterRequest, maximum_results: usize) -> Self {
        Self {
            protocol_version: CURRENT_GATEWAY_PROTOCOL_VERSION,
            query,
            filter,
            maximum_results,
        }
    }
}

/// Stable committed metadata returned for one document.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DocumentSummary {
    /// Canonical path relative to the content root.
    pub path: String,
    /// Directory-derived document classification.
    pub document_type: DocumentType,
    /// Directory-derived project, when the document belongs to a project.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<ProjectId>,
    /// Whether the canonical path is below an archive directory.
    pub archived: bool,
    /// SHA-256 revision of the exact Markdown bytes.
    pub revision: Revision,
    /// Validated YAML front matter.
    pub metadata: DocumentMetadata,
}

/// Successful response for committed list, recent, and search operations.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ListResponse {
    /// Independent Gateway protocol version.
    pub protocol_version: u16,
    /// Exact official Git commit used for every returned document.
    pub commit: String,
    /// Matching documents in operation-defined deterministic order.
    pub documents: Vec<DocumentSummary>,
}

impl ListResponse {
    /// Constructs a response using the current protocol version.
    #[must_use]
    pub const fn new(commit: String, documents: Vec<DocumentSummary>) -> Self {
        Self {
            protocol_version: CURRENT_GATEWAY_PROTOCOL_VERSION,
            commit,
            documents,
        }
    }
}

/// One committed Markdown document with its stable summary.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DocumentContent {
    /// Stable committed document metadata.
    pub summary: DocumentSummary,
    /// Exact UTF-8 Markdown including YAML front matter.
    pub markdown: String,
}

/// Successful response for one committed document lookup.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GetResponse {
    /// Independent Gateway protocol version.
    pub protocol_version: u16,
    /// Exact official Git commit used for the returned document.
    pub commit: String,
    /// Requested committed Markdown document.
    pub document: DocumentContent,
}

impl GetResponse {
    /// Constructs a response using the current protocol version.
    #[must_use]
    pub const fn new(commit: String, document: DocumentContent) -> Self {
        Self {
            protocol_version: CURRENT_GATEWAY_PROTOCOL_VERSION,
            commit,
            document,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ExportRequest, GetRequest, ListRequest, ReadFilterRequest, SearchRequest};

    #[test]
    fn read_requests_use_strict_versioned_shapes() {
        let list: ListRequest = serde_json::from_str(
            r#"{"protocol_version":1,"project":"fictional-project","tag":"cuda","maximum_results":20}"#,
        )
        .unwrap_or_else(|error| panic!("list request must decode: {error}"));
        assert_eq!(list.maximum_results, 20);
        assert!(list.filter.session.is_none());

        let search: SearchRequest = serde_json::from_str(
            r#"{"protocol_version":1,"query":"memory","include_archived":true,"maximum_results":10}"#,
        )
        .unwrap_or_else(|error| panic!("search request must decode: {error}"));
        assert!(search.filter.include_archived);

        for invalid in [
            r#"{"protocol_version":1,"maximum_results":20,"unknown":true}"#,
            r#"{"protocol_version":1,"query":"memory","maximum_results":10,"unknown":true}"#,
        ] {
            assert!(serde_json::from_str::<ListRequest>(invalid).is_err());
            assert!(serde_json::from_str::<SearchRequest>(invalid).is_err());
        }
        assert!(
            serde_json::from_str::<GetRequest>(
                r#"{"protocol_version":1,"document_id":"01K00000000000000000000000","extra":true}"#
            )
            .is_err()
        );
        assert!(
            serde_json::from_str::<ExportRequest>(
                r#"{"protocol_version":1,"document_id":"01K00000000000000000000000","extra":true}"#
            )
            .is_err()
        );
        assert!(!ReadFilterRequest::default().include_archived);
    }
}

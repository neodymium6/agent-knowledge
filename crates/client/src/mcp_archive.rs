use agent_knowledge_core::{
    CURRENT_PROTOCOL_VERSION, ChangeRequest, DocumentId, DocumentStatus, DocumentType, Operation,
    RequestId, Revision,
};
use agent_knowledge_protocol::{DocumentSummary, GetRequest};
use schemars::JsonSchema;
use serde::Deserialize;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::mcp_package::PreparedPackage;

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct ArchiveDocumentParameters {
    /// Permanent document ULID.
    document_id: String,
    /// SHA-256 revision returned by the most recent knowledge_get call.
    expected_revision: String,
    /// Request ULID. Omit to generate one; reuse it with every other input after an uncertain response.
    #[serde(default)]
    request_id: Option<String>,
    /// RFC 3339 request time. Omit to use the current time; reuse it with every other input after an uncertain response.
    #[serde(default)]
    created_at: Option<String>,
}

pub(super) struct ArchiveDocumentIntent {
    document_id: DocumentId,
    expected_revision: Revision,
    request_id: RequestId,
    created_at: OffsetDateTime,
}

impl ArchiveDocumentParameters {
    pub(super) fn parse(self) -> Result<ArchiveDocumentIntent, String> {
        let document_id = self
            .document_id
            .parse::<DocumentId>()
            .map_err(|_| "document_id must be a canonical ULID".to_owned())?;
        let expected_revision = self
            .expected_revision
            .parse::<Revision>()
            .map_err(|_| "expected_revision must be a canonical SHA-256 revision".to_owned())?;
        let request_id = self
            .request_id
            .map(|value| {
                value
                    .parse::<RequestId>()
                    .map_err(|_| "request_id must be a canonical ULID".to_owned())
            })
            .transpose()?
            .unwrap_or_else(RequestId::generate);
        let created_at = self
            .created_at
            .map(|value| {
                OffsetDateTime::parse(&value, &Rfc3339)
                    .map_err(|_| "created_at must be an RFC 3339 timestamp".to_owned())
            })
            .transpose()?
            .unwrap_or_else(OffsetDateTime::now_utc);
        Ok(ArchiveDocumentIntent {
            document_id,
            expected_revision,
            request_id,
            created_at,
        })
    }
}

impl ArchiveDocumentIntent {
    pub(super) const fn get_request(&self) -> GetRequest {
        GetRequest::new(self.document_id)
    }

    pub(super) fn prepare(self, summary: &DocumentSummary) -> Result<PreparedPackage, String> {
        if summary.revision != self.expected_revision {
            return Err(
                "expected_revision does not match the committed document revision".to_owned(),
            );
        }
        if summary.archived {
            return Err("document is already archived".to_owned());
        }
        if matches!(
            summary.document_type,
            DocumentType::Log | DocumentType::Index
        ) {
            return Err("logs and index documents cannot be archived".to_owned());
        }
        if summary.metadata.status != DocumentStatus::Active {
            return Err("only active documents can be archived".to_owned());
        }

        let request = ChangeRequest {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            request_id: self.request_id,
            title: format!("Archive document {}", self.document_id),
            project: summary.project.clone(),
            document_type: summary.document_type,
            node: None,
            agent: None,
            session: None,
            created_at: self.created_at,
            operations: vec![Operation::ArchiveDocument {
                document_id: self.document_id,
                expected_revision: self.expected_revision,
            }],
        };
        PreparedPackage::new(request, Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use agent_knowledge_core::{DocumentType, Operation};
    use agent_knowledge_protocol::DocumentSummary;
    use agent_knowledge_queue::{PackagePolicy, validate_package};

    use super::ArchiveDocumentParameters;

    const DOCUMENT_ID: &str = "01K00000000000000000000001";
    const REQUEST_ID: &str = "01K00000000000000000000002";
    const REVISION: &str =
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn parameters(expected_revision: &str) -> ArchiveDocumentParameters {
        serde_json::from_value(serde_json::json!({
            "document_id": DOCUMENT_ID,
            "expected_revision": expected_revision,
            "request_id": REQUEST_ID,
            "created_at": "2026-08-06T10:00:00Z"
        }))
        .unwrap_or_else(|error| panic!("archive parameters must decode: {error}"))
    }

    fn summary(document_type: &str, archived: bool, status: &str) -> DocumentSummary {
        serde_json::from_value(serde_json::json!({
            "path": "projects/fictional-solver/experiments/2026/08/fictional-result.md",
            "document_type": document_type,
            "project": "fictional-solver",
            "archived": archived,
            "revision": REVISION,
            "metadata": {
                "schema_version": 1,
                "document_id": DOCUMENT_ID,
                "title": "Fictional result",
                "created": "2026-08-05T10:00:00Z",
                "request_id": "01K00000000000000000000003",
                "status": status
            }
        }))
        .unwrap_or_else(|error| panic!("document summary must decode: {error}"))
    }

    #[test]
    fn prepares_one_deterministic_archive_operation() {
        let first = parameters(REVISION)
            .parse()
            .and_then(|intent| intent.prepare(&summary("experiment", false, "active")))
            .unwrap_or_else(|error| panic!("archive package must prepare: {error}"));
        let second = parameters(REVISION)
            .parse()
            .and_then(|intent| intent.prepare(&summary("experiment", false, "active")))
            .unwrap_or_else(|error| panic!("archive package must prepare again: {error}"));
        assert_eq!(first, second);

        let temporary = first
            .materialize()
            .unwrap_or_else(|error| panic!("archive package must materialize: {error}"));
        let validated = validate_package(temporary.path(), &PackagePolicy::default())
            .unwrap_or_else(|error| panic!("archive package must validate: {error}"));
        assert_eq!(validated.request().document_type, DocumentType::Experiment);
        assert_eq!(
            validated
                .request()
                .project
                .as_ref()
                .map(ToString::to_string)
                .as_deref(),
            Some("fictional-solver")
        );
        assert!(matches!(
            validated.request().operations.as_slice(),
            [Operation::ArchiveDocument { .. }]
        ));
    }

    #[test]
    fn rejects_stale_and_forbidden_documents() {
        let cases = [
            (
                parameters(
                    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                )
                .parse()
                .and_then(|intent| intent.prepare(&summary("experiment", false, "active"))),
                "expected_revision does not match the committed document revision",
            ),
            (
                parameters(REVISION)
                    .parse()
                    .and_then(|intent| intent.prepare(&summary("experiment", true, "archived"))),
                "document is already archived",
            ),
            (
                parameters(REVISION)
                    .parse()
                    .and_then(|intent| intent.prepare(&summary("log", false, "active"))),
                "logs and index documents cannot be archived",
            ),
            (
                parameters(REVISION)
                    .parse()
                    .and_then(|intent| intent.prepare(&summary("index", false, "active"))),
                "logs and index documents cannot be archived",
            ),
            (
                parameters(REVISION)
                    .parse()
                    .and_then(|intent| intent.prepare(&summary("runbook", false, "completed"))),
                "only active documents can be archived",
            ),
        ];

        for (result, expected) in cases {
            assert_eq!(result.err().as_deref(), Some(expected));
        }
    }
}

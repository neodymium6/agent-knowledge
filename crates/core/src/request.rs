use std::collections::HashSet;
use std::fmt;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::{
    AttachmentName, DocumentId, PathValidationError, PayloadPath, ProjectId, RequestId, Revision,
    SessionId,
};

/// The only protocol version supported by this implementation.
pub const CURRENT_PROTOCOL_VERSION: u16 = 1;

/// A built-in document classification with defined mutation semantics.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DocumentType {
    /// A project overview or current-status document.
    Index,
    /// An append-only coding-agent work log.
    Log,
    /// A mutable experiment and its artifacts.
    Experiment,
    /// A mutable decision record.
    Decision,
    /// A mutable operational procedure.
    Runbook,
    /// External research or supporting information.
    Reference,
}

/// One atomic content mutation inside a change request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Operation {
    /// Creates a Markdown document from one payload file.
    CreateDocument {
        /// The permanent ID written into document front matter.
        document_id: DocumentId,
        /// The Markdown source inside the request payload.
        content: PayloadPath,
    },
    /// Replaces a mutable Markdown document.
    UpdateDocument {
        /// The permanent document ID.
        document_id: DocumentId,
        /// The revision observed by the client.
        expected_revision: Revision,
        /// The replacement Markdown source inside the request payload.
        content: PayloadPath,
    },
    /// Moves a mutable document to a new classification.
    MoveDocument {
        /// The permanent document ID.
        document_id: DocumentId,
        /// The revision observed by the client.
        expected_revision: Revision,
        /// The destination project, or `None` for the inbox.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        project: Option<ProjectId>,
        /// The destination document type.
        document_type: DocumentType,
    },
    /// Moves a mutable document into its archive.
    ArchiveDocument {
        /// The permanent document ID.
        document_id: DocumentId,
        /// The revision observed by the client.
        expected_revision: Revision,
    },
    /// Adds one new attachment without overwriting an existing file.
    AddAttachment {
        /// The document whose directory receives the attachment.
        document_id: DocumentId,
        /// The attachment source inside the request payload.
        source: PayloadPath,
        /// The destination file name next to the document.
        name: AttachmentName,
    },
}

/// One client-authored logical change that must be applied atomically.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChangeRequest {
    /// The wire protocol version.
    pub protocol_version: u16,
    /// The idempotency key generated before connecting.
    pub request_id: RequestId,
    /// A meaningful title used in commit messages.
    pub title: String,
    /// The target project, or `None` when classification is incomplete.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<ProjectId>,
    /// The primary document type for path derivation and policy.
    pub document_type: DocumentType,
    /// The client node that produced the request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    /// The coding agent that produced the request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    /// The coding-agent work session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<SessionId>,
    /// The client creation time, including an explicit UTC offset.
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    /// Ordered operations evaluated as one atomic request.
    pub operations: Vec<Operation>,
}

impl ChangeRequest {
    /// Decodes a JSON request while preserving path-validation failures.
    ///
    /// # Errors
    ///
    /// Returns a structured JSON failure or the invalid typed path field.
    pub fn decode_json(input: &[u8]) -> Result<Self, RequestDecodeError> {
        let wire =
            serde_json::from_slice::<WireChangeRequest>(input).map_err(RequestDecodeError::Json)?;
        wire.try_into()
    }

    /// Validates request-level invariants that do not require repository state.
    ///
    /// # Errors
    ///
    /// Returns the first deterministic validation failure.
    pub fn validate(&self, limits: RequestLimits) -> Result<(), RequestValidationError> {
        if self.protocol_version != CURRENT_PROTOCOL_VERSION {
            return Err(RequestValidationError::UnsupportedProtocolVersion {
                found: self.protocol_version,
            });
        }

        let title = self.title.trim();
        if title.is_empty() {
            return Err(RequestValidationError::EmptyTitle);
        }
        if title.chars().any(char::is_control) {
            return Err(RequestValidationError::TitleContainsControlCharacter);
        }
        let title_characters = self.title.chars().count();
        if title_characters > limits.maximum_title_characters {
            return Err(RequestValidationError::TitleTooLong {
                maximum: limits.maximum_title_characters,
                actual: title_characters,
            });
        }

        if self.operations.is_empty() {
            return Err(RequestValidationError::EmptyOperations);
        }
        if self.operations.len() > limits.maximum_operations {
            return Err(RequestValidationError::TooManyOperations {
                maximum: limits.maximum_operations,
                actual: self.operations.len(),
            });
        }

        validate_optional_metadata("node", self.node.as_deref())?;
        validate_optional_metadata("agent", self.agent.as_deref())?;

        if self.document_type == DocumentType::Log {
            if self.node.is_none() {
                return Err(RequestValidationError::MissingLogMetadata { field: "node" });
            }
            if self.agent.is_none() {
                return Err(RequestValidationError::MissingLogMetadata { field: "agent" });
            }
            if self.session.is_none() {
                return Err(RequestValidationError::MissingLogMetadata { field: "session" });
            }
        }

        let mut created_documents = HashSet::new();
        for operation in &self.operations {
            if let Operation::CreateDocument { document_id, .. } = operation
                && !created_documents.insert(*document_id)
            {
                return Err(RequestValidationError::DuplicateCreatedDocument {
                    document_id: *document_id,
                });
            }
        }

        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireChangeRequest {
    protocol_version: u16,
    request_id: RequestId,
    title: String,
    #[serde(default)]
    project: Option<String>,
    document_type: DocumentType,
    #[serde(default)]
    node: Option<String>,
    #[serde(default)]
    agent: Option<String>,
    #[serde(default)]
    session: Option<SessionId>,
    #[serde(with = "time::serde::rfc3339")]
    created_at: OffsetDateTime,
    operations: Vec<WireOperation>,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum WireOperation {
    CreateDocument {
        document_id: DocumentId,
        content: String,
    },
    UpdateDocument {
        document_id: DocumentId,
        expected_revision: Revision,
        content: String,
    },
    MoveDocument {
        document_id: DocumentId,
        expected_revision: Revision,
        #[serde(default)]
        project: Option<String>,
        document_type: DocumentType,
    },
    ArchiveDocument {
        document_id: DocumentId,
        expected_revision: Revision,
    },
    AddAttachment {
        document_id: DocumentId,
        source: String,
        name: String,
    },
}

impl TryFrom<WireChangeRequest> for ChangeRequest {
    type Error = RequestDecodeError;

    fn try_from(wire: WireChangeRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            protocol_version: wire.protocol_version,
            request_id: wire.request_id,
            title: wire.title,
            project: parse_optional_path("project", wire.project)?,
            document_type: wire.document_type,
            node: wire.node,
            agent: wire.agent,
            session: wire.session,
            created_at: wire.created_at,
            operations: wire
                .operations
                .into_iter()
                .map(Operation::try_from)
                .collect::<Result<_, _>>()?,
        })
    }
}

impl TryFrom<WireOperation> for Operation {
    type Error = RequestDecodeError;

    fn try_from(wire: WireOperation) -> Result<Self, Self::Error> {
        match wire {
            WireOperation::CreateDocument {
                document_id,
                content,
            } => Ok(Self::CreateDocument {
                document_id,
                content: parse_path("content", content)?,
            }),
            WireOperation::UpdateDocument {
                document_id,
                expected_revision,
                content,
            } => Ok(Self::UpdateDocument {
                document_id,
                expected_revision,
                content: parse_path("content", content)?,
            }),
            WireOperation::MoveDocument {
                document_id,
                expected_revision,
                project,
                document_type,
            } => Ok(Self::MoveDocument {
                document_id,
                expected_revision,
                project: parse_optional_path("project", project)?,
                document_type,
            }),
            WireOperation::ArchiveDocument {
                document_id,
                expected_revision,
            } => Ok(Self::ArchiveDocument {
                document_id,
                expected_revision,
            }),
            WireOperation::AddAttachment {
                document_id,
                source,
                name,
            } => Ok(Self::AddAttachment {
                document_id,
                source: parse_path("source", source)?,
                name: parse_path("name", name)?,
            }),
        }
    }
}

fn parse_path<T>(field: &'static str, value: String) -> Result<T, RequestDecodeError>
where
    T: TryFrom<String, Error = PathValidationError>,
{
    T::try_from(value).map_err(|source| RequestDecodeError::InvalidPath { field, source })
}

fn parse_optional_path<T>(
    field: &'static str,
    value: Option<String>,
) -> Result<Option<T>, RequestDecodeError>
where
    T: TryFrom<String, Error = PathValidationError>,
{
    value.map(|value| parse_path(field, value)).transpose()
}

/// A deterministic failure while decoding request JSON into domain types.
#[derive(Debug)]
pub enum RequestDecodeError {
    /// JSON syntax or a non-path wire field was invalid.
    Json(serde_json::Error),
    /// A typed path field was not normalized or safe.
    InvalidPath {
        /// The wire field containing the invalid path.
        field: &'static str,
        /// The path invariant that failed.
        source: PathValidationError,
    },
}

impl fmt::Display for RequestDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json(error) => write!(formatter, "request JSON is invalid: {error}"),
            Self::InvalidPath { field, source } => {
                write!(
                    formatter,
                    "request path field `{field}` is invalid: {source}"
                )
            }
        }
    }
}

impl std::error::Error for RequestDecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Json(error) => Some(error),
            Self::InvalidPath { source, .. } => Some(source),
        }
    }
}

fn validate_optional_metadata(
    field: &'static str,
    value: Option<&str>,
) -> Result<(), RequestValidationError> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.trim().is_empty() || value.chars().any(char::is_control) {
        return Err(RequestValidationError::InvalidMetadata { field });
    }
    Ok(())
}

/// Configurable request limits used by deterministic validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestLimits {
    /// Maximum number of Unicode scalar values in a title.
    pub maximum_title_characters: usize,
    /// Maximum number of operations in one request.
    pub maximum_operations: usize,
}

impl Default for RequestLimits {
    fn default() -> Self {
        Self {
            maximum_title_characters: 200,
            maximum_operations: 100,
        }
    }
}

/// A deterministic request validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestValidationError {
    /// The protocol version is not supported.
    UnsupportedProtocolVersion {
        /// The received protocol version.
        found: u16,
    },
    /// The title was empty or whitespace-only.
    EmptyTitle,
    /// The title contained a control character.
    TitleContainsControlCharacter,
    /// The title exceeded the configured character limit.
    TitleTooLong {
        /// The configured limit.
        maximum: usize,
        /// The observed character count.
        actual: usize,
    },
    /// No operations were supplied.
    EmptyOperations,
    /// The operation count exceeded the configured limit.
    TooManyOperations {
        /// The configured limit.
        maximum: usize,
        /// The observed operation count.
        actual: usize,
    },
    /// A required log metadata field was absent.
    MissingLogMetadata {
        /// The missing field name.
        field: &'static str,
    },
    /// Optional metadata was present but empty or contained a control character.
    InvalidMetadata {
        /// The invalid field name.
        field: &'static str,
    },
    /// Two create operations used the same permanent document ID.
    DuplicateCreatedDocument {
        /// The duplicated document ID.
        document_id: DocumentId,
    },
}

impl fmt::Display for RequestValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedProtocolVersion { found } => write!(
                formatter,
                "unsupported protocol version {found}; expected {CURRENT_PROTOCOL_VERSION}"
            ),
            Self::EmptyTitle => formatter.write_str("request title must not be empty"),
            Self::TitleContainsControlCharacter => {
                formatter.write_str("request title must not contain control characters")
            }
            Self::TitleTooLong { maximum, actual } => write!(
                formatter,
                "request title contains {actual} characters; maximum is {maximum}"
            ),
            Self::EmptyOperations => {
                formatter.write_str("request must contain at least one operation")
            }
            Self::TooManyOperations { maximum, actual } => write!(
                formatter,
                "request contains {actual} operations; maximum is {maximum}"
            ),
            Self::MissingLogMetadata { field } => {
                write!(formatter, "log request requires `{field}` metadata")
            }
            Self::InvalidMetadata { field } => {
                write!(formatter, "request metadata `{field}` is invalid")
            }
            Self::DuplicateCreatedDocument { document_id } => {
                write!(
                    formatter,
                    "document ID `{document_id}` is created more than once"
                )
            }
        }
    }
}

impl std::error::Error for RequestValidationError {}

#[cfg(test)]
mod tests {
    use super::{
        CURRENT_PROTOCOL_VERSION, ChangeRequest, DocumentType, Operation, RequestDecodeError,
        RequestLimits, RequestValidationError,
    };
    use crate::{DocumentId, RequestId, SessionId};

    const REQUEST_JSON: &str = r#"{
        "protocol_version": 1,
        "request_id": "01K00000000000000000000000",
        "title": "Record GPU memory measurements",
        "project": "cuda-solver",
        "document_type": "experiment",
        "node": "fictional-supercomputer-a",
        "agent": "codex",
        "session": "01K00000000000000000000001",
        "created_at": "2026-07-31T03:50:00+09:00",
        "operations": [
            {
                "type": "create_document",
                "document_id": "01K00000000000000000000002",
                "content": "experiment/index.md"
            },
            {
                "type": "add_attachment",
                "document_id": "01K00000000000000000000002",
                "source": "experiment/results.csv",
                "name": "results.csv"
            }
        ]
    }"#;

    fn parse_fixture() -> ChangeRequest {
        match serde_json::from_str(REQUEST_JSON) {
            Ok(request) => request,
            Err(error) => panic!("request fixture must be valid: {error}"),
        }
    }

    #[test]
    fn request_json_round_trips() {
        let request = parse_fixture();
        assert_eq!(request.validate(RequestLimits::default()), Ok(()));

        let serialized = match serde_json::to_string(&request) {
            Ok(serialized) => serialized,
            Err(error) => panic!("request must serialize: {error}"),
        };
        let reparsed = match serde_json::from_str::<ChangeRequest>(&serialized) {
            Ok(reparsed) => reparsed,
            Err(error) => panic!("serialized request must parse: {error}"),
        };
        assert_eq!(reparsed, request);
    }

    #[test]
    fn request_decoder_preserves_invalid_path_failures() {
        let json = REQUEST_JSON.replace(
            "\"content\": \"experiment/index.md\"",
            "\"content\": \"../index.md\"",
        );
        let error = match ChangeRequest::decode_json(json.as_bytes()) {
            Ok(_) => panic!("traversal path must fail"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            RequestDecodeError::InvalidPath {
                field: "content",
                ..
            }
        ));
    }

    #[test]
    fn request_json_rejects_unknown_fields() {
        let json = REQUEST_JSON.replace(
            "\"title\": \"Record GPU memory measurements\",",
            "\"title\": \"Record GPU memory measurements\", \"unknown\": true,",
        );
        assert!(serde_json::from_str::<ChangeRequest>(&json).is_err());
    }

    #[test]
    fn operation_json_rejects_unknown_fields() {
        let json = REQUEST_JSON.replace(
            "\"content\": \"experiment/index.md\"",
            "\"content\": \"experiment/index.md\", \"destination\": \"/tmp\"",
        );
        assert!(serde_json::from_str::<ChangeRequest>(&json).is_err());
    }

    #[test]
    fn validation_rejects_unsupported_protocol_version() {
        let mut request = parse_fixture();
        request.protocol_version = CURRENT_PROTOCOL_VERSION + 1;
        assert_eq!(
            request.validate(RequestLimits::default()),
            Err(RequestValidationError::UnsupportedProtocolVersion {
                found: CURRENT_PROTOCOL_VERSION + 1
            })
        );
    }

    #[test]
    fn validation_rejects_empty_and_oversized_titles() {
        let mut request = parse_fixture();
        request.title = " \t ".into();
        assert_eq!(
            request.validate(RequestLimits::default()),
            Err(RequestValidationError::EmptyTitle)
        );

        request.title = "three".into();
        assert_eq!(
            request.validate(RequestLimits {
                maximum_title_characters: 4,
                ..RequestLimits::default()
            }),
            Err(RequestValidationError::TitleTooLong {
                maximum: 4,
                actual: 5
            })
        );

        request.title = "  ok  ".into();
        assert_eq!(
            request.validate(RequestLimits {
                maximum_title_characters: 4,
                ..RequestLimits::default()
            }),
            Err(RequestValidationError::TitleTooLong {
                maximum: 4,
                actual: 6
            })
        );
    }

    #[test]
    fn validation_rejects_log_requests_without_session_metadata() {
        let mut request = parse_fixture();
        request.document_type = DocumentType::Log;
        request.session = None;
        assert_eq!(
            request.validate(RequestLimits::default()),
            Err(RequestValidationError::MissingLogMetadata { field: "session" })
        );
    }

    #[test]
    fn validation_rejects_duplicate_document_creation() {
        let mut request = parse_fixture();
        let Some(Operation::CreateDocument { document_id, .. }) = request.operations.first() else {
            panic!("first fixture operation must create a document");
        };
        let document_id = *document_id;
        request.operations.push(Operation::CreateDocument {
            document_id,
            content: match "duplicate.md".parse() {
                Ok(path) => path,
                Err(error) => panic!("test payload path must be valid: {error}"),
            },
        });

        assert_eq!(
            request.validate(RequestLimits::default()),
            Err(RequestValidationError::DuplicateCreatedDocument { document_id })
        );
    }

    #[test]
    fn typed_ids_remain_distinct_in_request_model() {
        let request = parse_fixture();
        let _: RequestId = request.request_id;
        let _: Option<SessionId> = request.session;
        let Some(Operation::CreateDocument { document_id, .. }) = request.operations.first() else {
            panic!("first fixture operation must create a document");
        };
        let _: DocumentId = *document_id;
    }
}

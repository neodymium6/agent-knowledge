use std::fmt;
use std::fs;
use std::io;
use std::path::Path;

use agent_knowledge_core::{
    ChangeRequest, DocumentId, DocumentLimits, DocumentMetadata, DocumentValidationError,
    Operation, PayloadPath, RequestId,
};

pub(super) fn validate_documents(
    request: &ChangeRequest,
    payload_root: &Path,
    limits: DocumentLimits,
) -> Result<(), MarkdownValidationError> {
    for operation in &request.operations {
        match operation {
            Operation::CreateDocument {
                document_id,
                content,
            } => validate_document(payload_root, content, *document_id, request, limits, false)?,
            Operation::UpdateDocument {
                document_id,
                content,
                ..
            } => validate_document(payload_root, content, *document_id, request, limits, true)?,
            Operation::MoveDocument { .. }
            | Operation::ArchiveDocument { .. }
            | Operation::AddAttachment { .. } => {}
        }
    }
    Ok(())
}

fn validate_document(
    payload_root: &Path,
    payload_path: &PayloadPath,
    expected_document_id: DocumentId,
    request: &ChangeRequest,
    limits: DocumentLimits,
    updated_required: bool,
) -> Result<(), MarkdownValidationError> {
    let bytes =
        fs::read(payload_root.join(payload_path.as_str())).map_err(MarkdownValidationError::Io)?;
    let markdown = std::str::from_utf8(&bytes)
        .map_err(|_| MarkdownValidationError::InvalidUtf8(payload_path.clone()))?;
    let yaml = extract_front_matter(markdown, payload_path)?;
    let metadata = yaml_serde::from_str::<DocumentMetadata>(yaml).map_err(|source| {
        MarkdownValidationError::InvalidYaml {
            path: payload_path.clone(),
            source,
        }
    })?;
    metadata
        .validate(request.document_type, limits)
        .map_err(|source| MarkdownValidationError::InvalidMetadata {
            path: payload_path.clone(),
            source,
        })?;

    if metadata.document_id != expected_document_id {
        return Err(MarkdownValidationError::DocumentIdMismatch {
            path: payload_path.clone(),
            expected: expected_document_id,
            found: metadata.document_id,
        });
    }
    if metadata.request_id != request.request_id {
        return Err(MarkdownValidationError::RequestIdMismatch {
            path: payload_path.clone(),
            expected: request.request_id,
            found: metadata.request_id,
        });
    }
    validate_optional_request_metadata(
        payload_path,
        "node",
        metadata.node.as_deref(),
        request.node.as_deref(),
    )?;
    validate_optional_request_metadata(
        payload_path,
        "agent",
        metadata.agent.as_deref(),
        request.agent.as_deref(),
    )?;
    if let Some(session) = metadata.session
        && Some(session) != request.session
    {
        return Err(MarkdownValidationError::RequestMetadataMismatch {
            path: payload_path.clone(),
            field: "session",
        });
    }
    if updated_required && metadata.updated.is_none() {
        return Err(MarkdownValidationError::MissingUpdatedTimestamp(
            payload_path.clone(),
        ));
    }

    Ok(())
}

fn extract_front_matter<'a>(
    markdown: &'a str,
    path: &PayloadPath,
) -> Result<&'a str, MarkdownValidationError> {
    let remainder = markdown
        .strip_prefix("---\n")
        .or_else(|| markdown.strip_prefix("---\r\n"))
        .ok_or_else(|| MarkdownValidationError::MissingOpeningDelimiter(path.clone()))?;

    let mut offset = 0_usize;
    for line in remainder.split_inclusive('\n') {
        let without_newline = line.strip_suffix('\n').unwrap_or(line);
        let content = without_newline
            .strip_suffix('\r')
            .unwrap_or(without_newline);
        if content == "---" {
            return Ok(&remainder[..offset]);
        }
        offset += line.len();
    }

    Err(MarkdownValidationError::MissingClosingDelimiter(
        path.clone(),
    ))
}

fn validate_optional_request_metadata(
    path: &PayloadPath,
    field: &'static str,
    document_value: Option<&str>,
    request_value: Option<&str>,
) -> Result<(), MarkdownValidationError> {
    if let Some(document_value) = document_value
        && Some(document_value) != request_value
    {
        return Err(MarkdownValidationError::RequestMetadataMismatch {
            path: path.clone(),
            field,
        });
    }
    Ok(())
}

/// A Markdown front-matter parsing or consistency failure.
#[derive(Debug)]
pub enum MarkdownValidationError {
    /// Reading the Markdown payload failed.
    Io(io::Error),
    /// Markdown was not valid UTF-8.
    InvalidUtf8(PayloadPath),
    /// The initial `---` delimiter was absent.
    MissingOpeningDelimiter(PayloadPath),
    /// The closing `---` delimiter was absent.
    MissingClosingDelimiter(PayloadPath),
    /// YAML decoding failed.
    InvalidYaml {
        /// The Markdown payload path.
        path: PayloadPath,
        /// The YAML decoder error.
        source: yaml_serde::Error,
    },
    /// Typed front-matter validation failed.
    InvalidMetadata {
        /// The Markdown payload path.
        path: PayloadPath,
        /// The deterministic metadata error.
        source: DocumentValidationError,
    },
    /// Front matter and its operation named different documents.
    DocumentIdMismatch {
        /// The Markdown payload path.
        path: PayloadPath,
        /// The document ID declared by the operation.
        expected: DocumentId,
        /// The document ID found in front matter.
        found: DocumentId,
    },
    /// Front matter named a different change request.
    RequestIdMismatch {
        /// The Markdown payload path.
        path: PayloadPath,
        /// The package request ID.
        expected: RequestId,
        /// The request ID found in front matter.
        found: RequestId,
    },
    /// Optional identity metadata differed from the request.
    RequestMetadataMismatch {
        /// The Markdown payload path.
        path: PayloadPath,
        /// The mismatched field.
        field: &'static str,
    },
    /// An update omitted the required `updated` timestamp.
    MissingUpdatedTimestamp(PayloadPath),
}

impl MarkdownValidationError {
    /// Returns the stable protocol error code for this failure.
    #[must_use]
    pub const fn error_code(&self) -> agent_knowledge_core::ErrorCode {
        match self {
            Self::Io(_) => agent_knowledge_core::ErrorCode::TemporaryFailure,
            _ => agent_knowledge_core::ErrorCode::InvalidFrontMatter,
        }
    }
}

impl fmt::Display for MarkdownValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "Markdown I/O failed: {error}"),
            Self::InvalidUtf8(path) => write!(formatter, "`{path}` is not UTF-8"),
            Self::MissingOpeningDelimiter(path) => {
                write!(
                    formatter,
                    "`{path}` must start with a YAML front-matter delimiter"
                )
            }
            Self::MissingClosingDelimiter(path) => {
                write!(
                    formatter,
                    "`{path}` has no closing YAML front-matter delimiter"
                )
            }
            Self::InvalidYaml { path, source } => {
                write!(formatter, "`{path}` contains invalid YAML: {source}")
            }
            Self::InvalidMetadata { path, source } => {
                write!(formatter, "`{path}` contains invalid metadata: {source}")
            }
            Self::DocumentIdMismatch {
                path,
                expected,
                found,
            } => write!(
                formatter,
                "`{path}` has document ID `{found}`; operation requires `{expected}`"
            ),
            Self::RequestIdMismatch {
                path,
                expected,
                found,
            } => write!(
                formatter,
                "`{path}` has request ID `{found}`; package requires `{expected}`"
            ),
            Self::RequestMetadataMismatch { path, field } => {
                write!(
                    formatter,
                    "`{path}` metadata `{field}` differs from the request"
                )
            }
            Self::MissingUpdatedTimestamp(path) => {
                write!(
                    formatter,
                    "updated document `{path}` requires `updated` metadata"
                )
            }
        }
    }
}

impl std::error::Error for MarkdownValidationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::InvalidYaml { source, .. } => Some(source),
            Self::InvalidMetadata { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests;

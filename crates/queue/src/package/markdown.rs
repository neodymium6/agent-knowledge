use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt;
use std::fs;
use std::io;
use std::path::Path;
#[cfg(test)]
use std::{cell::Cell, thread_local};

use agent_knowledge_core::{
    ChangeRequest, DocumentId, DocumentLimits, DocumentMetadata, DocumentValidationError,
    Operation, PayloadPath, RequestId,
};

#[cfg(test)]
thread_local! {
    static DOCUMENT_PARSE_COUNT: Cell<usize> = const { Cell::new(0) };
}

pub(super) fn validate_documents(
    request: &ChangeRequest,
    payload_root: &Path,
    limits: DocumentLimits,
    maximum_front_matter_bytes: usize,
) -> Result<(), MarkdownValidationError> {
    let mut documents = HashMap::new();
    for operation in &request.operations {
        let (document_id, content, updated_required) = match operation {
            Operation::CreateDocument {
                document_id,
                content,
            } => (*document_id, content, false),
            Operation::UpdateDocument {
                document_id,
                content,
                ..
            } => (*document_id, content, true),
            Operation::MoveDocument { .. }
            | Operation::ArchiveDocument { .. }
            | Operation::AddAttachment { .. } => continue,
        };
        let metadata = match documents.entry(content.clone()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(parse_document(
                payload_root,
                content,
                request,
                limits,
                maximum_front_matter_bytes,
            )?),
        };
        validate_operation_metadata(metadata, content, document_id, updated_required)?;
    }

    Ok(())
}

fn parse_document(
    payload_root: &Path,
    payload_path: &PayloadPath,
    request: &ChangeRequest,
    limits: DocumentLimits,
    maximum_front_matter_bytes: usize,
) -> Result<DocumentMetadata, MarkdownValidationError> {
    #[cfg(test)]
    DOCUMENT_PARSE_COUNT.set(DOCUMENT_PARSE_COUNT.get() + 1);

    let bytes =
        fs::read(payload_root.join(payload_path.as_str())).map_err(MarkdownValidationError::Io)?;
    let markdown = std::str::from_utf8(&bytes)
        .map_err(|_| MarkdownValidationError::InvalidUtf8(payload_path.clone()))?;
    let yaml = extract_front_matter(markdown, payload_path, maximum_front_matter_bytes)?;
    let options = serde_saphyr::options! {
        budget: serde_saphyr::budget! {
            max_events: maximum_front_matter_bytes,
            max_aliases: 0,
            max_anchors: 0,
            max_depth: 16,
            max_documents: 1,
            max_nodes: maximum_front_matter_bytes,
            max_total_scalar_bytes: maximum_front_matter_bytes,
            max_total_comment_bytes: maximum_front_matter_bytes,
            max_merge_keys: 0,
        },
        merge_keys: serde_saphyr::MergeKeyPolicy::Error,
        strict_booleans: true,
        with_snippet: false,
    };
    let metadata = serde_saphyr::from_str_with_options::<DocumentMetadata>(yaml, options).map_err(
        |source| MarkdownValidationError::InvalidYaml {
            path: payload_path.clone(),
            source: Box::new(source),
        },
    )?;
    metadata
        .validate(request.document_type, limits)
        .map_err(|source| MarkdownValidationError::InvalidMetadata {
            path: payload_path.clone(),
            source,
        })?;

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
    Ok(metadata)
}

#[cfg(test)]
pub(super) fn reset_document_parse_count() {
    DOCUMENT_PARSE_COUNT.set(0);
}

#[cfg(test)]
pub(super) fn document_parse_count() -> usize {
    DOCUMENT_PARSE_COUNT.get()
}

fn validate_operation_metadata(
    metadata: &DocumentMetadata,
    payload_path: &PayloadPath,
    expected_document_id: DocumentId,
    updated_required: bool,
) -> Result<(), MarkdownValidationError> {
    if metadata.document_id != expected_document_id {
        return Err(MarkdownValidationError::DocumentIdMismatch {
            path: payload_path.clone(),
            expected: expected_document_id,
            found: metadata.document_id,
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
    maximum_bytes: usize,
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
            if offset > maximum_bytes {
                return Err(MarkdownValidationError::FrontMatterTooLarge {
                    path: path.clone(),
                    maximum: maximum_bytes,
                    actual: offset,
                });
            }
            return Ok(&remainder[..offset]);
        }
        offset += line.len();
        if offset > maximum_bytes {
            return Err(MarkdownValidationError::FrontMatterTooLarge {
                path: path.clone(),
                maximum: maximum_bytes,
                actual: offset,
            });
        }
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
        source: Box<serde_saphyr::Error>,
    },
    /// YAML front matter exceeded its configured byte limit.
    FrontMatterTooLarge {
        /// The Markdown payload path.
        path: PayloadPath,
        /// The configured maximum byte length.
        maximum: usize,
        /// The observed bytes before parsing stopped.
        actual: usize,
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
            Self::FrontMatterTooLarge {
                path,
                maximum,
                actual,
            } => write!(
                formatter,
                "`{path}` front matter is {actual} bytes; configured maximum is {maximum}"
            ),
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
            Self::InvalidYaml { source, .. } => Some(source.as_ref()),
            Self::InvalidMetadata { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests;

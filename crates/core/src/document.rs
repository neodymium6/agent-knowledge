use std::collections::HashSet;
use std::fmt;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::{DocumentId, DocumentStatus, DocumentType, RequestId, SessionId};

/// The only document front-matter schema version currently supported.
pub const CURRENT_DOCUMENT_SCHEMA_VERSION: u16 = 1;

/// Typed metadata stored in every Markdown document's YAML front matter.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DocumentMetadata {
    /// The front-matter schema version.
    pub schema_version: u16,
    /// The permanent document identifier.
    pub document_id: DocumentId,
    /// The human-readable document title.
    pub title: String,
    /// The document creation time with an explicit offset.
    #[serde(with = "time::serde::rfc3339")]
    pub created: OffsetDateTime,
    /// The most recent update time, when the document has been updated.
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub updated: Option<OffsetDateTime>,
    /// The client node associated with the document.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    /// The coding agent associated with the document.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    /// The coding-agent work session associated with the document.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<SessionId>,
    /// The request that created or most recently updated this content.
    pub request_id: RequestId,
    /// Cross-cutting classification labels.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    /// The document lifecycle status.
    pub status: DocumentStatus,
    /// The permanent ID of a replacement document.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_by: Option<DocumentId>,
}

impl DocumentMetadata {
    /// Validates front-matter invariants independent of repository state.
    ///
    /// # Errors
    ///
    /// Returns the first deterministic metadata validation failure.
    pub fn validate(
        &self,
        document_type: DocumentType,
        limits: DocumentLimits,
    ) -> Result<(), DocumentValidationError> {
        self.validate_common(limits)?;

        if document_type == DocumentType::Log {
            if self.node.is_none() {
                return Err(DocumentValidationError::MissingLogMetadata { field: "node" });
            }
            if self.agent.is_none() {
                return Err(DocumentValidationError::MissingLogMetadata { field: "agent" });
            }
            if self.session.is_none() {
                return Err(DocumentValidationError::MissingLogMetadata { field: "session" });
            }
        }

        Ok(())
    }

    /// Validates front-matter invariants shared by every document type.
    ///
    /// # Errors
    ///
    /// Returns the first deterministic metadata validation failure.
    pub fn validate_common(&self, limits: DocumentLimits) -> Result<(), DocumentValidationError> {
        if self.schema_version != CURRENT_DOCUMENT_SCHEMA_VERSION {
            return Err(DocumentValidationError::UnsupportedSchemaVersion {
                found: self.schema_version,
            });
        }

        validate_text("title", &self.title, limits.maximum_title_characters)?;
        validate_optional_text(
            "node",
            self.node.as_deref(),
            limits.maximum_metadata_characters,
        )?;
        validate_optional_text(
            "agent",
            self.agent.as_deref(),
            limits.maximum_metadata_characters,
        )?;

        if let Some(updated) = self.updated
            && updated < self.created
        {
            return Err(DocumentValidationError::UpdatedBeforeCreated);
        }
        if self.superseded_by == Some(self.document_id) {
            return Err(DocumentValidationError::SelfSuperseded);
        }

        if self.tags.len() > limits.maximum_tags {
            return Err(DocumentValidationError::TooManyTags {
                maximum: limits.maximum_tags,
                actual: self.tags.len(),
            });
        }
        let mut tags = HashSet::new();
        for tag in &self.tags {
            validate_text("tag", tag, limits.maximum_tag_characters)?;
            if !tags.insert(tag.as_str()) {
                return Err(DocumentValidationError::DuplicateTag(tag.clone()));
            }
        }

        Ok(())
    }
}

fn validate_optional_text(
    field: &'static str,
    value: Option<&str>,
    maximum: usize,
) -> Result<(), DocumentValidationError> {
    if let Some(value) = value {
        validate_text(field, value, maximum)?;
    }
    Ok(())
}

fn validate_text(
    field: &'static str,
    value: &str,
    maximum: usize,
) -> Result<(), DocumentValidationError> {
    if value.trim().is_empty() {
        return Err(DocumentValidationError::EmptyText { field });
    }
    if value.chars().any(char::is_control) {
        return Err(DocumentValidationError::ControlCharacter { field });
    }
    let actual = value.chars().count();
    if actual > maximum {
        return Err(DocumentValidationError::TextTooLong {
            field,
            maximum,
            actual,
        });
    }
    Ok(())
}

/// Configurable limits for document front matter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DocumentLimits {
    /// Maximum Unicode scalar values in a document title.
    pub maximum_title_characters: usize,
    /// Maximum Unicode scalar values in node and agent identifiers.
    pub maximum_metadata_characters: usize,
    /// Maximum number of tags.
    pub maximum_tags: usize,
    /// Maximum Unicode scalar values in one tag.
    pub maximum_tag_characters: usize,
}

impl Default for DocumentLimits {
    fn default() -> Self {
        Self {
            maximum_title_characters: 200,
            maximum_metadata_characters: 200,
            maximum_tags: 64,
            maximum_tag_characters: 64,
        }
    }
}

/// A deterministic document front-matter validation failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DocumentValidationError {
    /// The front-matter schema version is unsupported.
    UnsupportedSchemaVersion {
        /// The received schema version.
        found: u16,
    },
    /// A required string was empty or whitespace-only.
    EmptyText {
        /// The invalid field.
        field: &'static str,
    },
    /// A string contained a control character.
    ControlCharacter {
        /// The invalid field.
        field: &'static str,
    },
    /// A string exceeded its configured character limit.
    TextTooLong {
        /// The invalid field.
        field: &'static str,
        /// The configured maximum.
        maximum: usize,
        /// The observed count.
        actual: usize,
    },
    /// The update time preceded the creation time.
    UpdatedBeforeCreated,
    /// A document named itself as its replacement.
    SelfSuperseded,
    /// The number of tags exceeded its configured limit.
    TooManyTags {
        /// The configured maximum.
        maximum: usize,
        /// The observed count.
        actual: usize,
    },
    /// A tag appeared more than once.
    DuplicateTag(String),
    /// Required log metadata was absent.
    MissingLogMetadata {
        /// The missing field.
        field: &'static str,
    },
}

impl fmt::Display for DocumentValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedSchemaVersion { found } => write!(
                formatter,
                "unsupported document schema version {found}; expected {CURRENT_DOCUMENT_SCHEMA_VERSION}"
            ),
            Self::EmptyText { field } => write!(formatter, "`{field}` must not be empty"),
            Self::ControlCharacter { field } => {
                write!(formatter, "`{field}` must not contain control characters")
            }
            Self::TextTooLong {
                field,
                maximum,
                actual,
            } => write!(
                formatter,
                "`{field}` contains {actual} characters; maximum is {maximum}"
            ),
            Self::UpdatedBeforeCreated => {
                formatter.write_str("`updated` must not precede `created`")
            }
            Self::SelfSuperseded => formatter.write_str("a document must not supersede itself"),
            Self::TooManyTags { maximum, actual } => {
                write!(
                    formatter,
                    "document has {actual} tags; maximum is {maximum}"
                )
            }
            Self::DuplicateTag(tag) => write!(formatter, "tag `{tag}` appears more than once"),
            Self::MissingLogMetadata { field } => {
                write!(formatter, "log front matter requires `{field}`")
            }
        }
    }
}

impl std::error::Error for DocumentValidationError {}

#[cfg(test)]
mod tests;

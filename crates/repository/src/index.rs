use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use agent_knowledge_core::{
    DocumentId, DocumentLimits, DocumentMetadata, DocumentParseError, DocumentType,
    DocumentValidationError, ProjectId, Revision, decode_document_metadata,
};
use sha2::{Digest, Sha256};

/// Limits used while indexing one committed content tree.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContentPolicy {
    /// Maximum files and directories below the content root.
    pub maximum_entry_count: usize,
    /// Maximum bytes in one Markdown document.
    pub maximum_markdown_bytes: u64,
    /// Maximum bytes in one Markdown front matter section.
    pub maximum_front_matter_bytes: usize,
    /// Shared typed metadata limits.
    pub document: DocumentLimits,
}

impl Default for ContentPolicy {
    fn default() -> Self {
        Self {
            maximum_entry_count: 1_000_000,
            maximum_markdown_bytes: 32 * 1024 * 1024,
            maximum_front_matter_bytes: 64 * 1024,
            document: DocumentLimits::default(),
        }
    }
}

/// Indexed identity and revision for one canonical Markdown document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DocumentRecord {
    relative_path: PathBuf,
    location: DocumentLocation,
    metadata: DocumentMetadata,
    revision: Revision,
}

impl DocumentRecord {
    /// Returns the document path relative to the content root.
    #[must_use]
    pub fn relative_path(&self) -> &Path {
        &self.relative_path
    }

    /// Returns the classification derived from the canonical directory.
    #[must_use]
    pub const fn location(&self) -> &DocumentLocation {
        &self.location
    }

    /// Returns the decoded document metadata.
    #[must_use]
    pub const fn metadata(&self) -> &DocumentMetadata {
        &self.metadata
    }

    /// Returns the SHA-256 revision of the exact Markdown bytes.
    #[must_use]
    pub const fn revision(&self) -> Revision {
        self.revision
    }
}

/// Canonical directory-derived classification for one document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DocumentLocation {
    document_type: DocumentType,
    project: Option<ProjectId>,
    archived: bool,
}

impl DocumentLocation {
    /// Returns the document type encoded by its directory.
    #[must_use]
    pub const fn document_type(&self) -> DocumentType {
        self.document_type
    }

    /// Returns the project encoded by the directory, if any.
    #[must_use]
    pub const fn project(&self) -> Option<&ProjectId> {
        self.project.as_ref()
    }

    /// Returns whether the document is below an archive directory.
    #[must_use]
    pub const fn is_archived(&self) -> bool {
        self.archived
    }
}

/// A complete identity index for one committed content tree.
#[derive(Clone, Debug)]
pub struct ContentIndex {
    documents: HashMap<DocumentId, DocumentRecord>,
}

impl ContentIndex {
    /// Builds an index while rejecting unsafe entries and duplicate identities.
    ///
    /// Non-Markdown regular files are counted but are not decoded. Symbolic
    /// links and other special filesystem entries are rejected.
    ///
    /// # Errors
    ///
    /// Returns the first I/O, hierarchy, Markdown, metadata, or duplicate-ID
    /// failure.
    pub fn build(content_root: &Path, policy: ContentPolicy) -> Result<Self, ContentIndexError> {
        let root_metadata = fs::symlink_metadata(content_root).map_err(ContentIndexError::Io)?;
        if !root_metadata.file_type().is_dir() {
            return Err(ContentIndexError::InvalidRoot);
        }

        let mut documents = HashMap::new();
        let mut pending = vec![content_root.to_path_buf()];
        let mut entry_count = 0_usize;
        while let Some(directory) = pending.pop() {
            let mut entries = fs::read_dir(&directory)
                .map_err(ContentIndexError::Io)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(ContentIndexError::Io)?;
            entries.sort_by_key(fs::DirEntry::file_name);
            let mut child_directories = Vec::new();
            for entry in entries {
                entry_count =
                    entry_count
                        .checked_add(1)
                        .ok_or(ContentIndexError::EntryLimitExceeded {
                            maximum: policy.maximum_entry_count,
                        })?;
                if entry_count > policy.maximum_entry_count {
                    return Err(ContentIndexError::EntryLimitExceeded {
                        maximum: policy.maximum_entry_count,
                    });
                }

                let path = entry.path();
                let relative_path = path
                    .strip_prefix(content_root)
                    .map_err(|_| ContentIndexError::PathEscapedRoot)?
                    .to_path_buf();
                if relative_path.to_str().is_none() {
                    return Err(ContentIndexError::InvalidPathEncoding(relative_path));
                }

                let metadata = fs::symlink_metadata(&path).map_err(ContentIndexError::Io)?;
                if metadata.file_type().is_dir() {
                    child_directories.push(path);
                    continue;
                }
                if !metadata.file_type().is_file() {
                    return Err(ContentIndexError::InvalidEntryType(relative_path));
                }
                if path.extension().and_then(|value| value.to_str()) != Some("md") {
                    continue;
                }
                if metadata.len() > policy.maximum_markdown_bytes {
                    return Err(ContentIndexError::MarkdownTooLarge {
                        path: relative_path,
                        maximum: policy.maximum_markdown_bytes,
                        actual: metadata.len(),
                    });
                }

                let bytes = fs::read(&path).map_err(ContentIndexError::Io)?;
                let document = decode_document_metadata(&bytes, policy.maximum_front_matter_bytes)
                    .map_err(|source| ContentIndexError::InvalidDocument {
                        path: relative_path.clone(),
                        source,
                    })?;
                let location = classify_document_path(&relative_path)?;
                document
                    .validate(location.document_type, policy.document)
                    .map_err(|source| ContentIndexError::InvalidMetadata {
                        path: relative_path.clone(),
                        source,
                    })?;
                let revision = Revision::from_bytes(Sha256::digest(&bytes).into());
                let document_id = document.document_id;
                let record = DocumentRecord {
                    relative_path: relative_path.clone(),
                    location,
                    metadata: document,
                    revision,
                };
                if let Some(existing) = documents.insert(document_id, record) {
                    return Err(ContentIndexError::DuplicateDocumentId {
                        document_id,
                        first_path: existing.relative_path,
                        second_path: relative_path,
                    });
                }
            }
            pending.extend(child_directories.into_iter().rev());
        }

        Ok(Self { documents })
    }

    /// Returns the uniquely indexed document, if present.
    #[must_use]
    pub fn get(&self, document_id: DocumentId) -> Option<&DocumentRecord> {
        self.documents.get(&document_id)
    }

    /// Resolves a document and checks its exact byte revision.
    ///
    /// # Errors
    ///
    /// Returns `NotFound` or `RevisionConflict` without modifying content.
    pub fn require_revision(
        &self,
        document_id: DocumentId,
        expected_revision: Revision,
    ) -> Result<&DocumentRecord, RevisionCheckError> {
        let document = self
            .documents
            .get(&document_id)
            .ok_or(RevisionCheckError::NotFound { document_id })?;
        if document.revision != expected_revision {
            return Err(RevisionCheckError::RevisionConflict {
                document_id,
                expected: expected_revision,
                actual: document.revision,
            });
        }
        Ok(document)
    }

    /// Returns the number of indexed Markdown documents.
    #[must_use]
    pub fn len(&self) -> usize {
        self.documents.len()
    }

    /// Returns whether the content tree contains no Markdown documents.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.documents.is_empty()
    }
}

fn classify_document_path(path: &Path) -> Result<DocumentLocation, ContentIndexError> {
    let components = path
        .iter()
        .map(|component| {
            component
                .to_str()
                .ok_or_else(|| ContentIndexError::InvalidPathEncoding(path.to_path_buf()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let invalid = || ContentIndexError::InvalidDocumentPath(path.to_path_buf());

    match components.as_slice() {
        ["index.md"] => Ok(DocumentLocation {
            document_type: DocumentType::Index,
            project: None,
            archived: false,
        }),
        ["projects", project, "index.md"] => Ok(DocumentLocation {
            document_type: DocumentType::Index,
            project: Some(project.parse().map_err(|_| invalid())?),
            archived: false,
        }),
        [
            "projects",
            project,
            "archive",
            category,
            rest @ ..,
            "index.md",
        ] if !rest.is_empty() => Ok(DocumentLocation {
            document_type: parse_category(category).ok_or_else(invalid)?,
            project: Some(project.parse().map_err(|_| invalid())?),
            archived: true,
        }),
        ["projects", project, category, rest @ .., "index.md"] if !rest.is_empty() => {
            Ok(DocumentLocation {
                document_type: parse_category(category).ok_or_else(invalid)?,
                project: Some(project.parse().map_err(|_| invalid())?),
                archived: false,
            })
        }
        ["inbox", category, rest @ .., "index.md"] if !rest.is_empty() => Ok(DocumentLocation {
            document_type: parse_category(category).ok_or_else(invalid)?,
            project: None,
            archived: false,
        }),
        ["archive", category, rest @ .., "index.md"] if !rest.is_empty() => Ok(DocumentLocation {
            document_type: parse_category(category).ok_or_else(invalid)?,
            project: None,
            archived: true,
        }),
        _ => Err(invalid()),
    }
}

fn parse_category(category: &str) -> Option<DocumentType> {
    match category {
        "logs" => Some(DocumentType::Log),
        "experiments" => Some(DocumentType::Experiment),
        "decisions" => Some(DocumentType::Decision),
        "runbooks" => Some(DocumentType::Runbook),
        "references" => Some(DocumentType::Reference),
        _ => None,
    }
}

/// A deterministic optimistic-lock check failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RevisionCheckError {
    /// No document has the requested permanent identity.
    NotFound {
        /// Missing document identity.
        document_id: DocumentId,
    },
    /// The current exact bytes differ from the client revision.
    RevisionConflict {
        /// Conflicting document identity.
        document_id: DocumentId,
        /// Revision supplied by the client.
        expected: Revision,
        /// Revision calculated from committed content.
        actual: Revision,
    },
}

impl fmt::Display for RevisionCheckError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound { document_id } => {
                write!(formatter, "document `{document_id}` was not found")
            }
            Self::RevisionConflict {
                document_id,
                expected,
                actual,
            } => write!(
                formatter,
                "document `{document_id}` revision conflict: expected `{expected}`, found `{actual}`"
            ),
        }
    }
}

impl std::error::Error for RevisionCheckError {}

/// A canonical content hierarchy or index construction failure.
#[derive(Debug)]
pub enum ContentIndexError {
    /// A filesystem operation failed.
    Io(io::Error),
    /// The configured content root was not a real directory.
    InvalidRoot,
    /// A traversal unexpectedly escaped the configured root.
    PathEscapedRoot,
    /// A content path was not valid UTF-8.
    InvalidPathEncoding(PathBuf),
    /// A Markdown path did not follow the canonical classification layout.
    InvalidDocumentPath(PathBuf),
    /// A symbolic link or other special filesystem entry was present.
    InvalidEntryType(PathBuf),
    /// The hierarchy exceeded the configured entry limit.
    EntryLimitExceeded {
        /// Configured maximum entries.
        maximum: usize,
    },
    /// A Markdown document exceeded the configured byte limit.
    MarkdownTooLarge {
        /// Document path relative to the content root.
        path: PathBuf,
        /// Configured maximum bytes.
        maximum: u64,
        /// Observed bytes.
        actual: u64,
    },
    /// Markdown front matter could not be decoded safely.
    InvalidDocument {
        /// Document path relative to the content root.
        path: PathBuf,
        /// Decoding failure.
        source: DocumentParseError,
    },
    /// Typed document metadata violated a shared invariant.
    InvalidMetadata {
        /// Document path relative to the content root.
        path: PathBuf,
        /// Validation failure.
        source: DocumentValidationError,
    },
    /// Two Markdown files declared the same permanent identity.
    DuplicateDocumentId {
        /// Duplicated identity.
        document_id: DocumentId,
        /// First path in deterministic traversal order.
        first_path: PathBuf,
        /// Second path in deterministic traversal order.
        second_path: PathBuf,
    },
}

impl fmt::Display for ContentIndexError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "content index I/O failed: {error}"),
            Self::InvalidRoot => formatter.write_str("content root must be a real directory"),
            Self::PathEscapedRoot => formatter.write_str("content traversal escaped its root"),
            Self::InvalidPathEncoding(path) => {
                write!(formatter, "content path `{}` is not UTF-8", path.display())
            }
            Self::InvalidDocumentPath(path) => write!(
                formatter,
                "Markdown path `{}` is outside the canonical document layout",
                path.display()
            ),
            Self::InvalidEntryType(path) => write!(
                formatter,
                "content entry `{}` is not a regular file or directory",
                path.display()
            ),
            Self::EntryLimitExceeded { maximum } => {
                write!(formatter, "content hierarchy exceeds {maximum} entries")
            }
            Self::MarkdownTooLarge {
                path,
                maximum,
                actual,
            } => write!(
                formatter,
                "Markdown `{}` is {actual} bytes; maximum is {maximum}",
                path.display()
            ),
            Self::InvalidDocument { path, source } => {
                write!(
                    formatter,
                    "Markdown `{}` is invalid: {source}",
                    path.display()
                )
            }
            Self::InvalidMetadata { path, source } => write!(
                formatter,
                "Markdown `{}` metadata is invalid: {source}",
                path.display()
            ),
            Self::DuplicateDocumentId {
                document_id,
                first_path,
                second_path,
            } => write!(
                formatter,
                "document ID `{document_id}` appears in `{}` and `{}`",
                first_path.display(),
                second_path.display()
            ),
        }
    }
}

impl std::error::Error for ContentIndexError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::InvalidDocument { source, .. } => Some(source),
            Self::InvalidMetadata { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests;

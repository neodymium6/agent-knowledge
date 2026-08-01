use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::Instant;

use agent_knowledge_core::{
    AttachmentName, DocumentId, DocumentLimits, DocumentMetadata, DocumentParseError,
    DocumentStatus, DocumentType, DocumentValidationError, PayloadPath, ProjectId, Revision,
    decode_document_metadata,
};
use agent_knowledge_queue::PackagePolicy;
use sha2::{Digest, Sha256};

/// Limits used while indexing one committed content tree.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContentPolicy {
    /// Maximum files and directories below the content root.
    pub maximum_entry_count: usize,
    /// Maximum bytes in one Markdown document.
    pub maximum_markdown_bytes: u64,
    /// Maximum aggregate Markdown bytes inspected while building the index.
    pub maximum_total_markdown_bytes: u64,
    /// Maximum bytes in one Markdown front matter section.
    pub maximum_front_matter_bytes: usize,
    /// Shared typed metadata limits.
    pub document: DocumentLimits,
    /// Optional absolute deadline for one index scan.
    pub scan_deadline: Option<Instant>,
}

impl Default for ContentPolicy {
    fn default() -> Self {
        Self {
            maximum_entry_count: 1_000_000,
            maximum_markdown_bytes: 32 * 1024 * 1024,
            maximum_total_markdown_bytes: 8 * 1024 * 1024 * 1024,
            maximum_front_matter_bytes: 64 * 1024,
            document: DocumentLimits::default(),
            scan_deadline: None,
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
    /// Non-Markdown regular files must use an allowed attachment type and
    /// canonical location. Links, executable files, hard links, and other
    /// special filesystem entries are rejected.
    ///
    /// # Errors
    ///
    /// Returns the first I/O, hierarchy, Markdown, metadata, or duplicate-ID
    /// failure.
    pub fn build(
        content_root: &Path,
        policy: ContentPolicy,
        package_policy: &PackagePolicy,
    ) -> Result<Self, ContentIndexError> {
        let root_metadata = fs::symlink_metadata(content_root).map_err(ContentIndexError::Io)?;
        if !root_metadata.file_type().is_dir() {
            return Err(ContentIndexError::InvalidRoot);
        }

        Self::build_validated(content_root, policy, package_policy)
    }

    /// Builds an index below a descriptor-backed stable directory path.
    ///
    /// The supplied path may be a Linux `/proc` descriptor projection, so the
    /// root path itself is followed only after its identity is compared with
    /// the already-open directory descriptor. Descendants retain the normal
    /// strict no-link validation.
    ///
    /// # Errors
    ///
    /// Returns an error when the path and descriptor differ or normal content
    /// validation fails.
    pub fn build_from_pinned_root(
        content_root: &Path,
        pinned_root: &File,
        policy: ContentPolicy,
        package_policy: &PackagePolicy,
    ) -> Result<Self, ContentIndexError> {
        let root_metadata = fs::metadata(content_root).map_err(ContentIndexError::Io)?;
        let pinned_metadata = pinned_root.metadata().map_err(ContentIndexError::Io)?;
        if !root_metadata.file_type().is_dir()
            || !pinned_metadata.file_type().is_dir()
            || !same_root_identity(&root_metadata, &pinned_metadata)
        {
            return Err(ContentIndexError::InvalidRoot);
        }

        Self::build_validated(content_root, policy, package_policy)
    }

    fn build_validated(
        content_root: &Path,
        policy: ContentPolicy,
        package_policy: &PackagePolicy,
    ) -> Result<Self, ContentIndexError> {
        let mut documents = HashMap::<DocumentId, DocumentRecord>::new();
        let mut attachments = Vec::new();
        let mut pending = vec![content_root.to_path_buf()];
        let mut entry_count = 0_usize;
        let mut markdown_bytes = 0_u64;
        while let Some(directory) = pending.pop() {
            check_scan_deadline(policy.scan_deadline)?;
            let remaining = policy.maximum_entry_count.saturating_sub(entry_count);
            let mut entries = Vec::with_capacity(remaining.min(1_024));
            for entry in fs::read_dir(&directory).map_err(ContentIndexError::Io)? {
                let entry = entry.map_err(ContentIndexError::Io)?;
                if directory == content_root && entry.file_name() == ".git" {
                    continue;
                }
                entries.push(entry);
                if entries.len() > remaining {
                    break;
                }
            }
            if entries.len() > remaining {
                return Err(ContentIndexError::EntryLimitExceeded {
                    maximum: policy.maximum_entry_count,
                });
            }
            entries.sort_by_key(fs::DirEntry::file_name);
            let mut child_directories = Vec::new();
            for entry in entries {
                check_scan_deadline(policy.scan_deadline)?;
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
                validate_regular_file_metadata(&relative_path, &metadata)?;
                if path.extension().and_then(|value| value.to_str()) != Some("md") {
                    let maximum = package_policy.limits().maximum_file_bytes;
                    if metadata.len() > maximum {
                        return Err(ContentIndexError::AttachmentTooLarge {
                            path: relative_path,
                            maximum,
                            actual: metadata.len(),
                        });
                    }
                    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                        return Err(ContentIndexError::InvalidPathEncoding(relative_path));
                    };
                    if !package_policy.allows_attachment_name(name) {
                        return Err(ContentIndexError::UnsupportedAttachment(relative_path));
                    }
                    attachments.push(relative_path);
                    continue;
                }
                if metadata.len() > policy.maximum_markdown_bytes {
                    return Err(ContentIndexError::MarkdownTooLarge {
                        path: relative_path,
                        maximum: policy.maximum_markdown_bytes,
                        actual: metadata.len(),
                    });
                }
                markdown_bytes = markdown_bytes.checked_add(metadata.len()).ok_or(
                    ContentIndexError::MarkdownByteLimitExceeded {
                        maximum: policy.maximum_total_markdown_bytes,
                    },
                )?;
                if markdown_bytes > policy.maximum_total_markdown_bytes {
                    return Err(ContentIndexError::MarkdownByteLimitExceeded {
                        maximum: policy.maximum_total_markdown_bytes,
                    });
                }

                let bytes = read_bounded_file(&path, policy.maximum_markdown_bytes)?;
                if bytes.len() as u64 > policy.maximum_markdown_bytes {
                    return Err(ContentIndexError::MarkdownTooLarge {
                        path: relative_path,
                        maximum: policy.maximum_markdown_bytes,
                        actual: bytes.len() as u64,
                    });
                }
                let document = decode_document_metadata(&bytes, policy.maximum_front_matter_bytes)
                    .map_err(|source| ContentIndexError::InvalidDocument {
                        path: relative_path.clone(),
                        source,
                    })?;
                let location = classify_document_path(&relative_path)?;
                validate_canonical_document_path(&relative_path, &location, &document)?;
                if location.archived != (document.status == DocumentStatus::Archived) {
                    return Err(ContentIndexError::ArchiveStatusMismatch(relative_path));
                }
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

        validate_attachment_locations(&attachments, &documents, package_policy)?;

        Ok(Self { documents })
    }

    /// Returns the uniquely indexed document, if present.
    #[must_use]
    pub fn get(&self, document_id: DocumentId) -> Option<&DocumentRecord> {
        self.documents.get(&document_id)
    }

    /// Iterates over every indexed Markdown document.
    ///
    /// The iteration order is unspecified. Callers that expose results must
    /// apply an explicit deterministic ordering.
    pub fn documents(&self) -> impl Iterator<Item = &DocumentRecord> {
        self.documents.values()
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

fn check_scan_deadline(deadline: Option<Instant>) -> Result<(), ContentIndexError> {
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        return Err(ContentIndexError::ScanDeadlineExceeded);
    }
    Ok(())
}

#[cfg(unix)]
fn same_root_identity(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;

    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(not(unix))]
fn same_root_identity(_left: &fs::Metadata, _right: &fs::Metadata) -> bool {
    true
}

fn read_bounded_file(path: &Path, maximum: u64) -> Result<Vec<u8>, ContentIndexError> {
    let capacity = usize::try_from(maximum.min(64 * 1024)).unwrap_or(64 * 1024);
    let mut bytes = Vec::with_capacity(capacity);
    File::open(path)
        .and_then(|file| file.take(maximum.saturating_add(1)).read_to_end(&mut bytes))
        .map_err(ContentIndexError::Io)?;
    Ok(bytes)
}

fn validate_attachment_locations(
    attachments: &[PathBuf],
    documents: &HashMap<DocumentId, DocumentRecord>,
    package_policy: &PackagePolicy,
) -> Result<(), ContentIndexError> {
    let document_directories = documents
        .values()
        .filter_map(|document| document.relative_path.parent().map(Path::to_path_buf))
        .collect::<HashSet<_>>();
    for attachment in attachments {
        let beside_document = attachment
            .parent()
            .is_some_and(|parent| document_directories.contains(parent));
        let valid_name = attachment
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.parse::<AttachmentName>().is_ok());
        if !valid_name {
            return Err(ContentIndexError::InvalidAttachmentPath(attachment.clone()));
        }
        if !beside_document && !is_project_asset(attachment, package_policy) {
            return Err(ContentIndexError::OrphanAttachment(attachment.clone()));
        }
    }
    Ok(())
}

fn is_project_asset(path: &Path, package_policy: &PackagePolicy) -> bool {
    let components = path
        .iter()
        .filter_map(|component| component.to_str())
        .collect::<Vec<_>>();
    let ["projects", project, "assets", rest @ ..] = components.as_slice() else {
        return false;
    };
    !rest.is_empty()
        && rest.len() <= package_policy.limits().maximum_path_components
        && project.parse::<ProjectId>().is_ok()
        && rest
            .iter()
            .all(|component| component.parse::<AttachmentName>().is_ok())
        && rest.join("/").parse::<PayloadPath>().is_ok()
}

#[cfg(unix)]
fn validate_regular_file_metadata(
    path: &Path,
    metadata: &fs::Metadata,
) -> Result<(), ContentIndexError> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    if metadata.permissions().mode() & 0o111 != 0 {
        return Err(ContentIndexError::ExecutableFile(path.to_path_buf()));
    }
    if metadata.nlink() > 1 {
        return Err(ContentIndexError::HardLinkedFile(path.to_path_buf()));
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_regular_file_metadata(
    _path: &Path,
    _metadata: &fs::Metadata,
) -> Result<(), ContentIndexError> {
    Ok(())
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

fn validate_canonical_document_path(
    path: &Path,
    location: &DocumentLocation,
    metadata: &DocumentMetadata,
) -> Result<(), ContentIndexError> {
    let expected = canonical_document_path(location, metadata);
    if path != expected {
        return Err(ContentIndexError::InvalidDocumentPath(path.to_path_buf()));
    }
    Ok(())
}

fn canonical_document_path(location: &DocumentLocation, metadata: &DocumentMetadata) -> PathBuf {
    if location.document_type == DocumentType::Index {
        return location.project.as_ref().map_or_else(
            || PathBuf::from("index.md"),
            |project| {
                PathBuf::from("projects")
                    .join(project.as_str())
                    .join("index.md")
            },
        );
    }

    let category = category_name(location.document_type);
    let bundle = bundle_name(location.document_type, metadata);
    if location.archived {
        return if let Some(project) = location.project.as_ref() {
            PathBuf::from("projects")
                .join(project.as_str())
                .join("archive")
                .join(category)
                .join(bundle)
                .join("index.md")
        } else {
            PathBuf::from("archive")
                .join(category)
                .join(bundle)
                .join("index.md")
        };
    }

    let mut path = location.project.as_ref().map_or_else(
        || PathBuf::from("inbox").join(category),
        |project| {
            PathBuf::from("projects")
                .join(project.as_str())
                .join(category)
        },
    );
    if location.document_type == DocumentType::Log {
        path.push(format!("{:04}", metadata.created.year()));
        path.push(format!("{:02}", u8::from(metadata.created.month())));
        path.push(format!("{:02}", metadata.created.day()));
    }
    path.join(bundle).join("index.md")
}

fn bundle_name(document_type: DocumentType, metadata: &DocumentMetadata) -> String {
    if document_type == DocumentType::Log {
        format!(
            "{:02}{:02}{:02}-{}",
            metadata.created.hour(),
            metadata.created.minute(),
            metadata.created.second(),
            metadata.document_id
        )
    } else {
        format!(
            "{:04}-{:02}-{:02}-{}",
            metadata.created.year(),
            u8::from(metadata.created.month()),
            metadata.created.day(),
            metadata.document_id
        )
    }
}

const fn category_name(document_type: DocumentType) -> &'static str {
    match document_type {
        DocumentType::Index => "",
        DocumentType::Log => "logs",
        DocumentType::Experiment => "experiments",
        DocumentType::Decision => "decisions",
        DocumentType::Runbook => "runbooks",
        DocumentType::Reference => "references",
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
    /// A regular file had an executable mode bit.
    ExecutableFile(PathBuf),
    /// A regular file had more than one hard link.
    HardLinkedFile(PathBuf),
    /// A non-Markdown file used a disallowed extension.
    UnsupportedAttachment(PathBuf),
    /// An attachment path did not use safe visible components.
    InvalidAttachmentPath(PathBuf),
    /// One attachment exceeded the configured per-file byte limit.
    AttachmentTooLarge {
        /// Attachment path relative to the content root.
        path: PathBuf,
        /// Configured maximum bytes.
        maximum: u64,
        /// Observed bytes.
        actual: u64,
    },
    /// An attachment was neither beside a document nor in project assets.
    OrphanAttachment(PathBuf),
    /// Front-matter archive status disagreed with the canonical directory.
    ArchiveStatusMismatch(PathBuf),
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
    /// Aggregate Markdown bytes exceeded the configured index-work bound.
    MarkdownByteLimitExceeded {
        /// Configured maximum aggregate bytes.
        maximum: u64,
    },
    /// The configured absolute index-scan deadline expired.
    ScanDeadlineExceeded,
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
            Self::ExecutableFile(path) => {
                write!(formatter, "content file `{}` is executable", path.display())
            }
            Self::HardLinkedFile(path) => write!(
                formatter,
                "content file `{}` has multiple hard links",
                path.display()
            ),
            Self::UnsupportedAttachment(path) => write!(
                formatter,
                "content attachment `{}` has a disallowed extension",
                path.display()
            ),
            Self::InvalidAttachmentPath(path) => write!(
                formatter,
                "content attachment `{}` has an unsafe path",
                path.display()
            ),
            Self::AttachmentTooLarge {
                path,
                maximum,
                actual,
            } => write!(
                formatter,
                "content attachment `{}` is {actual} bytes; maximum is {maximum}",
                path.display()
            ),
            Self::OrphanAttachment(path) => write!(
                formatter,
                "content attachment `{}` is outside a document bundle or project assets",
                path.display()
            ),
            Self::ArchiveStatusMismatch(path) => write!(
                formatter,
                "document `{}` archive status disagrees with its path",
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
            Self::MarkdownByteLimitExceeded { maximum } => write!(
                formatter,
                "content hierarchy exceeds {maximum} aggregate Markdown bytes"
            ),
            Self::ScanDeadlineExceeded => {
                formatter.write_str("content index scan deadline expired")
            }
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

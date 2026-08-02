use std::ffi::OsStr;
use std::fmt;
use std::fs::{self, File, TryLockError};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use agent_knowledge_core::{
    DocumentId, PathAttestation, PathAttestationError, PinnedDirectory, PinnedPathError, ProjectId,
    Revision, SessionId, markdown_body,
};
use agent_knowledge_queue::PackagePolicy;
use sha2::{Digest, Sha256};

use crate::git::{
    ensure_canonical_worktree_clean_until, ensure_real_directory, ensure_supported_git_until,
    open_stable_directory, parse_object_id, run_git_for_read, validate_local_git_config_until,
    validate_repository_layout_until,
};
use crate::{
    AttachmentRecord, ContentIndex, ContentIndexError, ContentPolicy, DocumentRecord,
    GitTransactionError,
};

/// A validated read-only handle to the official repository and content tree.
#[derive(Clone, Debug)]
pub struct CommittedStore {
    git_directory: PathBuf,
    configured_git_directory: PathBuf,
    git_root_handle: Arc<File>,
    content_root: PathBuf,
    configured_content_root: PathBuf,
    content_root_handle: Arc<File>,
    official_ref: String,
}

impl CommittedStore {
    /// Returns attested identities for the bare repository and content root.
    ///
    /// # Errors
    ///
    /// Returns an error when either pinned storage binding changed or Linux
    /// mount topology cannot be inspected.
    pub fn storage_attestations(&self) -> Result<[PathAttestation; 2], PathAttestationError> {
        Ok([
            PathAttestation::capture(&self.configured_git_directory, &self.git_root_handle)?,
            PathAttestation::capture(&self.configured_content_root, &self.content_root_handle)?,
        ])
    }

    /// Opens a read boundary for one bare repository and its canonical worktree.
    ///
    /// # Errors
    ///
    /// Returns an error when Git is unsupported, a path is unsafe, the
    /// worktree is unrelated, or the official branch is invalid.
    pub fn open(
        git_directory: &Path,
        content_root: &Path,
        official_branch: &str,
    ) -> Result<Self, CommittedReadError> {
        Self::open_until(git_directory, content_root, official_branch, None)
    }

    /// Opens the committed store while bounding read-only Git inspection by
    /// an optional absolute deadline.
    pub fn open_until(
        git_directory: &Path,
        content_root: &Path,
        official_branch: &str,
        deadline: Option<Instant>,
    ) -> Result<Self, CommittedReadError> {
        check_operation_deadline(deadline)?;
        ensure_supported_git_until(deadline).map_err(CommittedReadError::repository)?;
        ensure_real_directory(git_directory).map_err(CommittedReadError::repository)?;
        ensure_real_directory(content_root).map_err(CommittedReadError::repository)?;
        let configured_git_directory =
            fs::canonicalize(git_directory).map_err(CommittedReadError::Io)?;
        let configured_content_root =
            fs::canonicalize(content_root).map_err(CommittedReadError::Io)?;
        if configured_git_directory.starts_with(&configured_content_root)
            || configured_content_root.starts_with(&configured_git_directory)
        {
            return Err(CommittedReadError::OverlappingPaths);
        }
        if official_branch.is_empty() || official_branch.chars().any(char::is_control) {
            return Err(CommittedReadError::InvalidOfficialBranch);
        }
        let official_ref = format!("refs/heads/{official_branch}");
        let (git_root_handle, stable_git_directory) =
            open_stable_directory(git_directory).map_err(CommittedReadError::repository)?;
        let (content_root_handle, stable_content_root) =
            open_stable_directory(content_root).map_err(CommittedReadError::repository)?;
        run_git_for_read(
            None,
            None,
            [OsStr::new("check-ref-format"), OsStr::new(&official_ref)],
            deadline,
        )
        .map_err(CommittedReadError::repository)?;
        validate_local_git_config_until(&stable_git_directory, deadline)
            .map_err(CommittedReadError::repository)?;
        validate_repository_layout_until(
            &stable_git_directory,
            &stable_content_root,
            &official_ref,
            deadline,
        )
        .map_err(CommittedReadError::repository)?;
        check_operation_deadline(deadline)?;

        Ok(Self {
            git_directory: stable_git_directory,
            configured_git_directory,
            git_root_handle,
            content_root: stable_content_root,
            configured_content_root,
            content_root_handle,
            official_ref,
        })
    }

    /// Pins one exact official commit and builds its validated content index.
    ///
    /// The shared content lock prevents the Repository Worker from advancing
    /// or synchronizing the canonical worktree until the snapshot is dropped.
    /// Repository transaction preparation remains independent. Lock
    /// contention during the short publication boundary fails immediately.
    ///
    /// # Errors
    ///
    /// Returns an error for lock contention, replaced storage, a dirty or
    /// stale worktree, invalid committed content, or Git and filesystem I/O.
    pub fn snapshot(
        &self,
        content_policy: ContentPolicy,
        package_policy: &PackagePolicy,
    ) -> Result<CommittedSnapshot, CommittedReadError> {
        let deadline = content_policy.scan_deadline;
        let (content_lock, official) = self.pin_current_commit(deadline)?;

        let index = ContentIndex::build_from_pinned_root(
            &self.content_root,
            &self.content_root_handle,
            content_policy,
            package_policy,
        )
        .map_err(CommittedReadError::content)?;
        let root = PinnedDirectory::try_clone_from(&self.content_root_handle)
            .map_err(CommittedReadError::PinnedPath)?;
        Ok(CommittedSnapshot {
            commit: official,
            index,
            root,
            maximum_markdown_bytes: content_policy.maximum_markdown_bytes,
            maximum_bundle_bytes: package_policy.limits().maximum_total_bytes,
            maximum_bundle_entries: package_policy.limits().maximum_file_count,
            deadline,
            _content_lock: content_lock,
        })
    }

    /// Returns the exact official commit checked out in canonical content.
    ///
    /// Unlike [`Self::snapshot`], this validates publication consistency
    /// without building a Markdown index.
    ///
    /// # Errors
    ///
    /// Returns an error for lock contention, replaced storage, a dirty or
    /// stale worktree, deadline expiry, or Git and filesystem I/O.
    pub fn current_commit_until(
        &self,
        deadline: Option<Instant>,
    ) -> Result<String, CommittedReadError> {
        self.pin_current_commit(deadline)
            .map(|(_lock, commit)| commit)
    }

    fn pin_current_commit(
        &self,
        deadline: Option<Instant>,
    ) -> Result<(File, String), CommittedReadError> {
        let content_lock = File::open(&self.content_root).map_err(CommittedReadError::Io)?;
        match content_lock.try_lock_shared() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Err(CommittedReadError::Busy),
            Err(TryLockError::Error(error)) => return Err(CommittedReadError::Io(error)),
        }
        validate_same_directory(&self.configured_git_directory, &self.git_root_handle)?;
        validate_same_directory(&self.configured_content_root, &self.content_root_handle)?;
        check_operation_deadline(deadline)?;
        validate_local_git_config_until(&self.git_directory, deadline)
            .map_err(CommittedReadError::repository)?;
        validate_repository_layout_until(
            &self.git_directory,
            &self.content_root,
            &self.official_ref,
            deadline,
        )
        .map_err(CommittedReadError::repository)?;
        ensure_canonical_worktree_clean_until(&self.content_root, deadline)
            .map_err(CommittedReadError::repository)?;

        let official = run_git_for_read(
            None,
            Some(&self.git_directory),
            [
                OsStr::new("rev-parse"),
                OsStr::new("--verify"),
                OsStr::new(&self.official_ref),
            ],
            deadline,
        )
        .and_then(|output| parse_object_id(&output.stdout))
        .map_err(CommittedReadError::repository)?;
        let checked_out = run_git_for_read(
            Some(&self.content_root),
            None,
            [
                OsStr::new("rev-parse"),
                OsStr::new("--verify"),
                OsStr::new("HEAD"),
            ],
            deadline,
        )
        .and_then(|output| parse_object_id(&output.stdout))
        .map_err(CommittedReadError::repository)?;
        if checked_out != official {
            return Err(CommittedReadError::CanonicalOutOfDate {
                official,
                checked_out,
            });
        }
        Ok((content_lock, official))
    }
}

fn validate_same_directory(configured: &Path, pinned: &File) -> Result<(), CommittedReadError> {
    let configured = fs::symlink_metadata(configured).map_err(CommittedReadError::Io)?;
    let pinned = pinned.metadata().map_err(CommittedReadError::Io)?;
    if !configured.file_type().is_dir() || !same_directory_metadata(&configured, &pinned) {
        return Err(CommittedReadError::StorageReplaced);
    }
    Ok(())
}

#[cfg(unix)]
fn same_directory_metadata(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;

    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(not(unix))]
fn same_directory_metadata(_left: &fs::Metadata, _right: &fs::Metadata) -> bool {
    true
}

/// One immutable view of an exact official commit.
#[derive(Debug)]
pub struct CommittedSnapshot {
    commit: String,
    index: ContentIndex,
    root: PinnedDirectory,
    maximum_markdown_bytes: u64,
    maximum_bundle_bytes: u64,
    maximum_bundle_entries: usize,
    deadline: Option<Instant>,
    _content_lock: File,
}

impl CommittedSnapshot {
    /// Returns the exact official commit pinned by this snapshot.
    #[must_use]
    pub fn commit(&self) -> &str {
        &self.commit
    }

    /// Lists matching documents in canonical path order.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested result bound is zero.
    pub fn list(
        &self,
        filter: &ReadFilter,
        maximum_results: usize,
    ) -> Result<Vec<&DocumentRecord>, CommittedReadError> {
        check_operation_deadline(self.deadline)?;
        validate_result_limit(maximum_results)?;
        let mut documents = self
            .index
            .documents()
            .filter(|document| filter.matches(document))
            .collect::<Vec<_>>();
        check_operation_deadline(self.deadline)?;
        documents.sort_by(|left, right| left.relative_path().cmp(right.relative_path()));
        check_operation_deadline(self.deadline)?;
        documents.truncate(maximum_results);
        Ok(documents)
    }

    /// Lists matching documents from most recently changed to oldest.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested result bound is zero.
    pub fn recent(
        &self,
        filter: &ReadFilter,
        maximum_results: usize,
    ) -> Result<Vec<&DocumentRecord>, CommittedReadError> {
        check_operation_deadline(self.deadline)?;
        validate_result_limit(maximum_results)?;
        let mut documents = self
            .index
            .documents()
            .filter(|document| filter.matches(document))
            .collect::<Vec<_>>();
        check_operation_deadline(self.deadline)?;
        documents.sort_by(|left, right| {
            let left_time = left.metadata().updated.unwrap_or(left.metadata().created);
            let right_time = right.metadata().updated.unwrap_or(right.metadata().created);
            right_time.cmp(&left_time).then_with(|| {
                left.metadata()
                    .document_id
                    .cmp(&right.metadata().document_id)
            })
        });
        check_operation_deadline(self.deadline)?;
        documents.truncate(maximum_results);
        Ok(documents)
    }

    /// Retrieves exact Markdown bytes for one permanent document identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the document is absent or the pinned bytes no
    /// longer match the validated committed index.
    pub fn get(
        &self,
        document_id: DocumentId,
    ) -> Result<CommittedDocument<'_>, CommittedReadError> {
        check_operation_deadline(self.deadline)?;
        let record = self
            .index
            .get(document_id)
            .ok_or(CommittedReadError::DocumentNotFound { document_id })?;
        let markdown = self.read_markdown(record)?;
        check_operation_deadline(self.deadline)?;
        Ok(CommittedDocument { record, markdown })
    }

    /// Retrieves one document and every attachment stored beside it.
    ///
    /// Entries use names relative to the bundle directory and are ordered
    /// deterministically with `index.md` first and attachments by name.
    ///
    /// # Errors
    ///
    /// Returns an error when the document is absent, the bundle exceeds the
    /// configured package bounds, or any file changed after indexing.
    pub fn bundle(
        &self,
        document_id: DocumentId,
    ) -> Result<CommittedBundle<'_>, CommittedReadError> {
        check_operation_deadline(self.deadline)?;
        let record = self
            .index
            .get(document_id)
            .ok_or(CommittedReadError::DocumentNotFound { document_id })?;
        let mut attachment_records = self.index.attachments_beside(record).collect::<Vec<_>>();
        attachment_records.sort_by(|left, right| left.relative_path().cmp(right.relative_path()));
        let entry_count = attachment_records.len().checked_add(1).ok_or(
            CommittedReadError::BundleEntryLimitExceeded {
                maximum: self.maximum_bundle_entries,
            },
        )?;
        if entry_count > self.maximum_bundle_entries {
            return Err(CommittedReadError::BundleEntryLimitExceeded {
                maximum: self.maximum_bundle_entries,
            });
        }

        let mut total_bytes = record.byte_length();
        for attachment in &attachment_records {
            total_bytes = total_bytes.checked_add(attachment.byte_length()).ok_or(
                CommittedReadError::BundleByteLimitExceeded {
                    maximum: self.maximum_bundle_bytes,
                },
            )?;
            if total_bytes > self.maximum_bundle_bytes {
                return Err(CommittedReadError::BundleByteLimitExceeded {
                    maximum: self.maximum_bundle_bytes,
                });
            }
        }

        let mut entries = Vec::with_capacity(entry_count);
        entries.push(CommittedBundleEntry {
            name: PathBuf::from("index.md"),
            bytes: self.read_markdown(record)?,
        });
        for attachment in attachment_records {
            let first = self.read_attachment(record, attachment)?;
            let first_revision = Revision::from_bytes(Sha256::digest(&first.bytes).into());
            drop(first);
            let second = self.read_attachment(record, attachment)?;
            let second_revision = Revision::from_bytes(Sha256::digest(&second.bytes).into());
            if first_revision != second_revision {
                return Err(CommittedReadError::ContentChanged { document_id });
            }
            entries.push(second);
        }
        check_operation_deadline(self.deadline)?;
        Ok(CommittedBundle { record, entries })
    }

    fn read_markdown(&self, record: &DocumentRecord) -> Result<Vec<u8>, CommittedReadError> {
        check_operation_deadline(self.deadline)?;
        let mut file = self
            .root
            .open_regular_beneath(record.relative_path())
            .map_err(CommittedReadError::PinnedPath)?;
        validate_pinned_content_file(&file).map_err(CommittedReadError::Io)?;
        if file.byte_length() > self.maximum_markdown_bytes {
            return Err(CommittedReadError::ContentChanged {
                document_id: record.metadata().document_id,
            });
        }
        let capacity = usize::try_from(file.byte_length().min(64 * 1024)).unwrap_or(64 * 1024);
        let mut markdown = Vec::with_capacity(capacity);
        let mut bounded = file
            .by_ref()
            .take(self.maximum_markdown_bytes.saturating_add(1));
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            check_operation_deadline(self.deadline)?;
            let count = bounded.read(&mut buffer).map_err(CommittedReadError::Io)?;
            if count == 0 {
                break;
            }
            markdown.extend_from_slice(&buffer[..count]);
        }
        check_operation_deadline(self.deadline)?;
        let revision = Revision::from_bytes(Sha256::digest(&markdown).into());
        if markdown.len() as u64 != file.byte_length() || revision != record.revision() {
            return Err(CommittedReadError::ContentChanged {
                document_id: record.metadata().document_id,
            });
        }
        Ok(markdown)
    }

    fn read_attachment(
        &self,
        document: &DocumentRecord,
        attachment: &AttachmentRecord,
    ) -> Result<CommittedBundleEntry, CommittedReadError> {
        check_operation_deadline(self.deadline)?;
        let mut file = self
            .root
            .open_regular_beneath(attachment.relative_path())
            .map_err(CommittedReadError::PinnedPath)?;
        validate_pinned_content_file(&file).map_err(CommittedReadError::Io)?;
        if file.byte_length() != attachment.byte_length() {
            return Err(CommittedReadError::ContentChanged {
                document_id: document.metadata().document_id,
            });
        }
        let capacity = usize::try_from(file.byte_length().min(64 * 1024)).unwrap_or(64 * 1024);
        let mut bytes = Vec::with_capacity(capacity);
        let mut bounded = file
            .by_ref()
            .take(attachment.byte_length().saturating_add(1));
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            check_operation_deadline(self.deadline)?;
            let count = bounded.read(&mut buffer).map_err(CommittedReadError::Io)?;
            if count == 0 {
                break;
            }
            bytes.extend_from_slice(&buffer[..count]);
        }
        if bytes.len() as u64 != attachment.byte_length() {
            return Err(CommittedReadError::ContentChanged {
                document_id: document.metadata().document_id,
            });
        }
        let name = attachment
            .relative_path()
            .file_name()
            .map(PathBuf::from)
            .ok_or(CommittedReadError::ContentChanged {
                document_id: document.metadata().document_id,
            })?;
        Ok(CommittedBundleEntry { name, bytes })
    }
}

#[cfg(unix)]
fn validate_pinned_content_file(file: &agent_knowledge_core::PinnedRegularFile) -> io::Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let metadata = file.metadata()?;
    if metadata.nlink() != 1 || metadata.permissions().mode() & 0o111 != 0 {
        return Err(io::Error::other(
            "committed content file is linked or executable",
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_pinned_content_file(_file: &agent_knowledge_core::PinnedRegularFile) -> io::Result<()> {
    Ok(())
}

fn validate_result_limit(maximum_results: usize) -> Result<(), CommittedReadError> {
    if maximum_results == 0 {
        Err(CommittedReadError::InvalidResultLimit)
    } else {
        Ok(())
    }
}

fn check_operation_deadline(deadline: Option<Instant>) -> Result<(), CommittedReadError> {
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        Err(CommittedReadError::OperationDeadlineExceeded)
    } else {
        Ok(())
    }
}

/// Exact Markdown bytes and validated metadata for one committed document.
#[derive(Debug)]
pub struct CommittedDocument<'a> {
    record: &'a DocumentRecord,
    markdown: Vec<u8>,
}

/// One immutable committed document bundle prepared for deterministic export.
#[derive(Debug)]
pub struct CommittedBundle<'a> {
    record: &'a DocumentRecord,
    entries: Vec<CommittedBundleEntry>,
}

impl CommittedBundle<'_> {
    /// Returns the indexed document record owning this bundle.
    #[must_use]
    pub const fn record(&self) -> &DocumentRecord {
        self.record
    }

    /// Returns the complete, deterministic bundle entry sequence.
    #[must_use]
    pub fn entries(&self) -> &[CommittedBundleEntry] {
        &self.entries
    }

    /// Consumes the bundle and returns its owned entries.
    #[must_use]
    pub fn into_entries(self) -> Vec<CommittedBundleEntry> {
        self.entries
    }
}

/// One regular file in a committed document bundle.
#[derive(Debug)]
pub struct CommittedBundleEntry {
    name: PathBuf,
    bytes: Vec<u8>,
}

impl CommittedBundleEntry {
    /// Returns the single-component path relative to the bundle directory.
    #[must_use]
    pub fn name(&self) -> &Path {
        &self.name
    }

    /// Returns the exact committed file bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl CommittedDocument<'_> {
    /// Returns the indexed document record.
    #[must_use]
    pub const fn record(&self) -> &DocumentRecord {
        self.record
    }

    /// Returns the exact committed Markdown bytes.
    #[must_use]
    pub fn markdown(&self) -> &[u8] {
        &self.markdown
    }
}

/// Optional exact-match filters shared by list, recent, and search operations.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReadFilter {
    project: Option<ProjectId>,
    tag: Option<String>,
    session: Option<SessionId>,
    include_archived: bool,
}

impl ReadFilter {
    /// Creates one validated filter from typed identities and an exact tag.
    #[must_use]
    pub fn new(
        project: Option<ProjectId>,
        tag: Option<String>,
        session: Option<SessionId>,
        include_archived: bool,
    ) -> Self {
        Self {
            project,
            tag,
            session,
            include_archived,
        }
    }

    fn matches(&self, document: &DocumentRecord) -> bool {
        (self.include_archived || !document.location().is_archived())
            && self
                .project
                .as_ref()
                .is_none_or(|project| document.location().project() == Some(project))
            && self.tag.as_ref().is_none_or(|tag| {
                document
                    .metadata()
                    .tags
                    .iter()
                    .any(|candidate| candidate == tag)
            })
            && self
                .session
                .is_none_or(|session| document.metadata().session == Some(session))
    }
}

/// Allowlisted optional metadata searched by the initial linear backend.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SearchMetadataFields {
    node: bool,
    agent: bool,
    session: bool,
    request_id: bool,
}

impl SearchMetadataFields {
    /// Selects the optional metadata fields included in full-text matching.
    #[must_use]
    pub const fn new(node: bool, agent: bool, session: bool, request_id: bool) -> Self {
        Self {
            node,
            agent,
            session,
            request_id,
        }
    }
}

impl Default for SearchMetadataFields {
    fn default() -> Self {
        Self::new(true, true, true, true)
    }
}

/// Replaceable search boundary over an exact committed snapshot.
pub trait SearchBackend {
    /// Returns matching documents in deterministic canonical path order.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty or oversized query, a zero result bound,
    /// or a committed-content read failure.
    fn search<'a>(
        &self,
        snapshot: &'a CommittedSnapshot,
        query: &str,
        filter: &ReadFilter,
        policy: SearchPolicy,
    ) -> Result<Vec<&'a DocumentRecord>, CommittedReadError>;
}

/// CPU and I/O bounds for one search over a committed snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SearchPolicy {
    /// Maximum Unicode scalar values in the query.
    pub maximum_query_characters: usize,
    /// Maximum matching documents returned.
    pub maximum_results: usize,
    /// Maximum filtered documents inspected.
    pub maximum_scanned_documents: usize,
    /// Maximum Markdown bytes read for body matching.
    pub maximum_scanned_markdown_bytes: u64,
    /// Optional absolute deadline shared with snapshot construction.
    pub deadline: Option<Instant>,
}

impl SearchPolicy {
    /// Creates a policy with bounded query/results and unrestricted scan work.
    #[must_use]
    pub const fn new(maximum_query_characters: usize, maximum_results: usize) -> Self {
        Self {
            maximum_query_characters,
            maximum_results,
            maximum_scanned_documents: usize::MAX,
            maximum_scanned_markdown_bytes: u64::MAX,
            deadline: None,
        }
    }
}

/// Initial bounded full-text search that scans committed Markdown directly.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LinearSearch {
    metadata_fields: SearchMetadataFields,
}

impl LinearSearch {
    /// Creates a linear backend with an allowlisted metadata-field selection.
    #[must_use]
    pub const fn new(metadata_fields: SearchMetadataFields) -> Self {
        Self { metadata_fields }
    }
}

impl Default for LinearSearch {
    fn default() -> Self {
        Self::new(SearchMetadataFields::default())
    }
}

impl SearchBackend for LinearSearch {
    fn search<'a>(
        &self,
        snapshot: &'a CommittedSnapshot,
        query: &str,
        filter: &ReadFilter,
        policy: SearchPolicy,
    ) -> Result<Vec<&'a DocumentRecord>, CommittedReadError> {
        validate_result_limit(policy.maximum_results)?;
        let query = query.trim();
        if query.is_empty() {
            return Err(CommittedReadError::EmptyQuery);
        }
        let actual = query.chars().count();
        if actual > policy.maximum_query_characters {
            return Err(CommittedReadError::QueryTooLong {
                maximum: policy.maximum_query_characters,
                actual,
            });
        }
        let query = query.to_lowercase();
        let mut matches = Vec::new();
        let mut scanned_documents = 0_usize;
        let mut scanned_markdown_bytes = 0_u64;
        for record in snapshot
            .index
            .documents()
            .filter(|record| filter.matches(record))
        {
            check_operation_deadline(policy.deadline)?;
            scanned_documents = scanned_documents.saturating_add(1);
            if scanned_documents > policy.maximum_scanned_documents {
                return Err(CommittedReadError::SearchDocumentLimitExceeded {
                    maximum: policy.maximum_scanned_documents,
                });
            }
            let metadata = record.metadata();
            let fixed_match = metadata.title.to_lowercase().contains(&query)
                || record
                    .relative_path()
                    .to_string_lossy()
                    .to_lowercase()
                    .contains(&query)
                || metadata
                    .tags
                    .iter()
                    .any(|tag| tag.to_lowercase().contains(&query));
            let optional_match = (self.metadata_fields.node
                && metadata
                    .node
                    .as_deref()
                    .is_some_and(|value| value.to_lowercase().contains(&query)))
                || (self.metadata_fields.agent
                    && metadata
                        .agent
                        .as_deref()
                        .is_some_and(|value| value.to_lowercase().contains(&query)))
                || (self.metadata_fields.session
                    && metadata
                        .session
                        .is_some_and(|value| value.to_string().to_lowercase().contains(&query)))
                || (self.metadata_fields.request_id
                    && metadata
                        .request_id
                        .to_string()
                        .to_lowercase()
                        .contains(&query));
            let markdown_match = if fixed_match || optional_match {
                false
            } else {
                let remaining = policy
                    .maximum_scanned_markdown_bytes
                    .saturating_sub(scanned_markdown_bytes);
                if record.byte_length() > remaining {
                    return Err(CommittedReadError::SearchMarkdownByteLimitExceeded {
                        maximum: policy.maximum_scanned_markdown_bytes,
                    });
                }
                let markdown = snapshot.read_markdown(record)?;
                scanned_markdown_bytes = scanned_markdown_bytes
                    .checked_add(markdown.len() as u64)
                    .ok_or(CommittedReadError::SearchMarkdownByteLimitExceeded {
                        maximum: policy.maximum_scanned_markdown_bytes,
                    })?;
                if scanned_markdown_bytes > policy.maximum_scanned_markdown_bytes {
                    return Err(CommittedReadError::SearchMarkdownByteLimitExceeded {
                        maximum: policy.maximum_scanned_markdown_bytes,
                    });
                }
                markdown_body(&markdown)
                    .map_err(|_| CommittedReadError::InvalidMarkdownEncoding {
                        document_id: metadata.document_id,
                    })?
                    .to_lowercase()
                    .contains(&query)
            };
            if fixed_match || optional_match || markdown_match {
                matches.push(record);
            }
        }
        matches.sort_by(|left, right| left.relative_path().cmp(right.relative_path()));
        check_operation_deadline(policy.deadline)?;
        matches.truncate(policy.maximum_results);
        Ok(matches)
    }
}

/// Failure while opening or querying an exact committed read snapshot.
#[derive(Debug)]
pub enum CommittedReadError {
    /// Repository validation or Git execution failed.
    Repository(Box<GitTransactionError>),
    /// Filesystem I/O failed outside repository validation.
    Io(io::Error),
    /// The repository and content roots overlap.
    OverlappingPaths,
    /// The configured official branch is empty, malformed, or unsafe.
    InvalidOfficialBranch,
    /// The configured repository or content root was replaced after opening.
    StorageReplaced,
    /// The Repository Worker currently owns the writer lock.
    Busy,
    /// The canonical worktree does not represent the current official commit.
    CanonicalOutOfDate {
        /// Commit named by the official ref.
        official: String,
        /// Commit checked out in the canonical worktree.
        checked_out: String,
    },
    /// The committed content hierarchy failed validation.
    Content(Box<ContentIndexError>),
    /// Descriptor-contained path resolution failed.
    PinnedPath(PinnedPathError),
    /// No committed document has the requested permanent identity.
    DocumentNotFound {
        /// Missing permanent identity.
        document_id: DocumentId,
    },
    /// Content changed after its committed index record was created.
    ContentChanged {
        /// Affected permanent identity.
        document_id: DocumentId,
    },
    /// A Markdown body unexpectedly ceased to be UTF-8.
    InvalidMarkdownEncoding {
        /// Affected permanent identity.
        document_id: DocumentId,
    },
    /// Search text was empty after trimming.
    EmptyQuery,
    /// Search text exceeded the configured character bound.
    QueryTooLong {
        /// Maximum accepted Unicode scalar values.
        maximum: usize,
        /// Supplied Unicode scalar values.
        actual: usize,
    },
    /// A list or search operation requested no possible results.
    InvalidResultLimit,
    /// Search inspected more documents than deployment policy permits.
    SearchDocumentLimitExceeded {
        /// Configured maximum inspected documents.
        maximum: usize,
    },
    /// Search read more Markdown body bytes than deployment policy permits.
    SearchMarkdownByteLimitExceeded {
        /// Configured maximum inspected Markdown bytes.
        maximum: u64,
    },
    /// A document bundle contained more files than package policy permits.
    BundleEntryLimitExceeded {
        /// Configured maximum bundle files.
        maximum: usize,
    },
    /// A document bundle contained more bytes than package policy permits.
    BundleByteLimitExceeded {
        /// Configured maximum uncompressed file bytes.
        maximum: u64,
    },
    /// The configured absolute committed-read deadline expired.
    OperationDeadlineExceeded,
}

impl fmt::Display for CommittedReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Repository(error) => {
                write!(formatter, "committed repository read failed: {error}")
            }
            Self::Io(error) => write!(formatter, "committed content I/O failed: {error}"),
            Self::OverlappingPaths => {
                formatter.write_str("repository and committed content roots must not overlap")
            }
            Self::InvalidOfficialBranch => formatter.write_str("official Git branch is invalid"),
            Self::StorageReplaced => {
                formatter.write_str("committed repository storage was replaced")
            }
            Self::Busy => formatter.write_str("Repository Worker is publishing committed content"),
            Self::CanonicalOutOfDate {
                official,
                checked_out,
            } => write!(
                formatter,
                "canonical worktree commit `{checked_out}` does not match official commit `{official}`"
            ),
            Self::Content(error) => write!(formatter, "committed content is invalid: {error}"),
            Self::PinnedPath(error) => write!(formatter, "committed path is invalid: {error}"),
            Self::DocumentNotFound { document_id } => {
                write!(formatter, "document `{document_id}` was not found")
            }
            Self::ContentChanged { document_id } => {
                write!(
                    formatter,
                    "document `{document_id}` changed during a committed read"
                )
            }
            Self::InvalidMarkdownEncoding { document_id } => {
                write!(formatter, "document `{document_id}` is not UTF-8")
            }
            Self::EmptyQuery => formatter.write_str("search query must not be empty"),
            Self::QueryTooLong { maximum, actual } => {
                write!(
                    formatter,
                    "search query has {actual} characters; maximum is {maximum}"
                )
            }
            Self::InvalidResultLimit => formatter.write_str("maximum results must be positive"),
            Self::SearchDocumentLimitExceeded { maximum } => {
                write!(formatter, "search exceeds {maximum} inspected documents")
            }
            Self::SearchMarkdownByteLimitExceeded { maximum } => {
                write!(
                    formatter,
                    "search exceeds {maximum} inspected Markdown bytes"
                )
            }
            Self::BundleEntryLimitExceeded { maximum } => {
                write!(formatter, "document bundle exceeds {maximum} files")
            }
            Self::BundleByteLimitExceeded { maximum } => {
                write!(formatter, "document bundle exceeds {maximum} bytes")
            }
            Self::OperationDeadlineExceeded => {
                formatter.write_str("committed read deadline expired")
            }
        }
    }
}

impl CommittedReadError {
    fn repository(error: GitTransactionError) -> Self {
        Self::Repository(Box::new(error))
    }

    fn content(error: ContentIndexError) -> Self {
        Self::Content(Box::new(error))
    }
}

impl std::error::Error for CommittedReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Repository(error) => Some(error),
            Self::Io(error) => Some(error),
            Self::Content(error) => Some(error),
            Self::PinnedPath(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests;

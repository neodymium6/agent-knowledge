use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use agent_knowledge_core::{
    DocumentId, DocumentMetadata, DocumentParseError, DocumentStatus, DocumentType,
    DocumentValidationError, ErrorCode, Operation, ProjectId, Revision, decode_document_metadata,
};
use agent_knowledge_queue::{
    ClaimedPackage, PackagePolicy, PackageValidationError, ValidatedPackage,
    validate_accepted_package,
};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;

use crate::{ContentIndex, ContentIndexError, ContentPolicy};

/// Result of applying one complete request to an isolated content tree.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ApplyOutcome {
    #[cfg(test)]
    operations_applied: usize,
    moves: Vec<AppliedMove>,
}

impl ApplyOutcome {
    /// Returns the number of request operations applied.
    #[cfg(test)]
    #[must_use]
    pub const fn operations_applied(&self) -> usize {
        self.operations_applied
    }

    pub(super) fn moves(&self) -> &[AppliedMove] {
        &self.moves
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct AppliedMove {
    pub(super) source: PathBuf,
    pub(super) destination: PathBuf,
}

/// Applies one Worker-owned request package to an isolated content tree.
///
/// All deterministic preconditions are checked before the first filesystem
/// mutation. An I/O failure can leave the isolated tree partially modified;
/// callers must discard or reset that worktree before processing another
/// request.
///
/// # Errors
///
/// Returns a deterministic request failure or a transient filesystem failure.
pub(crate) fn apply_claimed(
    content_root: &Path,
    claim: &ClaimedPackage,
    policy: ContentPolicy,
    package_policy: &PackagePolicy,
) -> Result<ApplyOutcome, ApplyError> {
    let revalidated = validate_accepted_package(claim.package_root(), package_policy)
        .map_err(ApplyError::PackageValidation)?;
    if &revalidated != claim.package() {
        return Err(ApplyError::ClaimPackageChanged);
    }
    apply_request_with_policy(
        content_root,
        claim.package_root(),
        &revalidated,
        policy,
        package_policy,
    )
}

#[cfg(test)]
fn apply_request(
    content_root: &Path,
    package_root: &Path,
    package: &ValidatedPackage,
    policy: ContentPolicy,
) -> Result<ApplyOutcome, ApplyError> {
    apply_request_with_policy(
        content_root,
        package_root,
        package,
        policy,
        &PackagePolicy::default(),
    )
}

fn apply_request_with_policy(
    content_root: &Path,
    package_root: &Path,
    package: &ValidatedPackage,
    policy: ContentPolicy,
    package_policy: &PackagePolicy,
) -> Result<ApplyOutcome, ApplyError> {
    let index = ContentIndex::build(content_root, policy, package_policy)
        .map_err(ApplyError::ContentIndex)?;
    let plan = build_plan(content_root, package_root, package, &index, policy)?;
    #[cfg(test)]
    let operations_applied = package.request().operations.len();
    let moves = execute_plan(content_root, plan)?;
    ContentIndex::build(content_root, policy, package_policy)
        .map_err(ApplyError::ResultingContent)?;
    Ok(ApplyOutcome {
        #[cfg(test)]
        operations_applied,
        moves,
    })
}

#[derive(Clone, Debug)]
struct VirtualDocument {
    relative_path: PathBuf,
    document_type: DocumentType,
    project: Option<ProjectId>,
    created: OffsetDateTime,
    updated: Option<OffsetDateTime>,
    archived: bool,
    status: DocumentStatus,
    revision: Revision,
    markdown: Vec<u8>,
}

enum PlannedMutation {
    WriteNew {
        relative_path: PathBuf,
        bytes: Vec<u8>,
    },
    Replace {
        relative_path: PathBuf,
        bytes: Vec<u8>,
    },
    Move {
        source: PathBuf,
        destination: PathBuf,
    },
}

fn build_plan(
    content_root: &Path,
    package_root: &Path,
    package: &ValidatedPackage,
    index: &ContentIndex,
    policy: ContentPolicy,
) -> Result<Vec<PlannedMutation>, ApplyError> {
    let request = package.request();
    let operation_time = package
        .acceptance()
        .map_or(request.created_at, |acceptance| acceptance.accepted_at);
    if request.created_at > operation_time {
        return Err(ApplyError::RequestCreatedAfterAcceptance);
    }
    let mut plan = Vec::with_capacity(request.operations.len());
    let mut documents = HashMap::<DocumentId, VirtualDocument>::new();
    let mut occupancy = VirtualOccupancy::new(content_root);

    for operation in &request.operations {
        match operation {
            Operation::CreateDocument {
                document_id,
                content,
            } => {
                if documents.contains_key(document_id) || index.get(*document_id).is_some() {
                    return Err(ApplyError::DocumentIdConflict {
                        document_id: *document_id,
                    });
                }
                let bytes = read_payload(package_root, package, content.as_str())?;
                let metadata = decode_payload_metadata(&bytes, policy, content.as_str())?;
                if metadata.created > operation_time
                    || metadata
                        .updated
                        .is_some_and(|updated| updated > operation_time)
                {
                    return Err(ApplyError::OperationForbidden {
                        document_id: *document_id,
                        detail: "document timestamps cannot follow request acceptance",
                    });
                }
                if metadata.status == DocumentStatus::Archived {
                    return Err(ApplyError::OperationForbidden {
                        document_id: *document_id,
                        detail: "documents cannot be created with archived status",
                    });
                }
                let relative_path = create_document_path(
                    request.project.as_ref(),
                    request.document_type,
                    metadata.created,
                    *document_id,
                );
                occupancy.reserve(&bundle_path(&relative_path, request.document_type))?;
                plan.push(PlannedMutation::WriteNew {
                    relative_path: relative_path.clone(),
                    bytes: bytes.clone(),
                });
                documents.insert(
                    *document_id,
                    VirtualDocument {
                        relative_path,
                        document_type: request.document_type,
                        project: request.project.clone(),
                        created: metadata.created,
                        updated: metadata.updated,
                        archived: false,
                        status: metadata.status,
                        revision: revision(&bytes),
                        markdown: bytes,
                    },
                );
            }
            Operation::UpdateDocument {
                document_id,
                expected_revision,
                content,
            } => {
                let document =
                    resolve_document(&mut documents, index, content_root, policy, *document_id)?;
                require_revision(document, *document_id, *expected_revision)?;
                require_mutable(
                    document,
                    *document_id,
                    request.project.as_ref(),
                    request.document_type,
                )?;
                let bytes = read_payload(package_root, package, content.as_str())?;
                let metadata = decode_payload_metadata(&bytes, policy, content.as_str())?;
                if metadata.created != document.created {
                    return Err(ApplyError::OperationForbidden {
                        document_id: *document_id,
                        detail: "document creation time is immutable",
                    });
                }
                if metadata.status == DocumentStatus::Archived {
                    return Err(ApplyError::OperationForbidden {
                        document_id: *document_id,
                        detail: "archive status requires an archive operation",
                    });
                }
                let previous_update = document.updated.unwrap_or(document.created);
                let Some(updated) = metadata.updated else {
                    return Err(ApplyError::OperationForbidden {
                        document_id: *document_id,
                        detail: "updated documents require an update time",
                    });
                };
                if updated > operation_time {
                    return Err(ApplyError::OperationForbidden {
                        document_id: *document_id,
                        detail: "document update time cannot follow request acceptance",
                    });
                }
                if updated <= previous_update {
                    return Err(ApplyError::OperationForbidden {
                        document_id: *document_id,
                        detail: "document update time must increase",
                    });
                }
                plan.push(PlannedMutation::Replace {
                    relative_path: document.relative_path.clone(),
                    bytes: bytes.clone(),
                });
                document.status = metadata.status;
                document.updated = metadata.updated;
                document.revision = revision(&bytes);
                document.markdown = bytes;
            }
            Operation::MoveDocument {
                document_id,
                expected_revision,
                project,
                document_type,
            } => {
                if *document_type != request.document_type || project != &request.project {
                    return Err(ApplyError::OperationForbidden {
                        document_id: *document_id,
                        detail: "move destination differs from request classification",
                    });
                }
                if matches!(document_type, DocumentType::Log | DocumentType::Index) {
                    return Err(ApplyError::OperationForbidden {
                        document_id: *document_id,
                        detail: "documents cannot be moved into log or index classification",
                    });
                }
                let document =
                    resolve_document(&mut documents, index, content_root, policy, *document_id)?;
                require_revision(document, *document_id, *expected_revision)?;
                require_movable(document, *document_id)?;
                let destination = create_document_path(
                    project.as_ref(),
                    *document_type,
                    document.created,
                    *document_id,
                );
                let source_bundle = bundle_path(&document.relative_path, document.document_type);
                let destination_bundle = bundle_path(&destination, *document_type);
                occupancy.move_path(&source_bundle, &destination_bundle)?;
                plan.push(PlannedMutation::Move {
                    source: source_bundle,
                    destination: destination_bundle,
                });
                document.relative_path = destination;
                document.document_type = *document_type;
                document.project.clone_from(project);
            }
            Operation::ArchiveDocument {
                document_id,
                expected_revision,
            } => {
                let document =
                    resolve_document(&mut documents, index, content_root, policy, *document_id)?;
                require_revision(document, *document_id, *expected_revision)?;
                require_classification(
                    document,
                    *document_id,
                    request.project.as_ref(),
                    request.document_type,
                )?;
                require_movable(document, *document_id)?;
                let destination = archive_document_path(document, *document_id);
                let destination_bundle = bundle_path(&destination, document.document_type);
                let source_bundle = bundle_path(&document.relative_path, document.document_type);
                occupancy.move_path(&source_bundle, &destination_bundle)?;
                let archived = archived_markdown(
                    &document.markdown,
                    policy,
                    request.request_id,
                    operation_time,
                )?;
                let archived_revision = revision(&archived);
                plan.push(PlannedMutation::Replace {
                    relative_path: document.relative_path.clone(),
                    bytes: archived.clone(),
                });
                plan.push(PlannedMutation::Move {
                    source: source_bundle,
                    destination: destination_bundle,
                });
                document.relative_path = destination;
                document.archived = true;
                document.status = DocumentStatus::Archived;
                document.updated = Some(operation_time);
                document.revision = archived_revision;
                document.markdown = archived;
            }
            Operation::AddAttachment {
                document_id,
                source,
                name,
            } => {
                let document =
                    resolve_document(&mut documents, index, content_root, policy, *document_id)?;
                require_accessible(
                    document,
                    *document_id,
                    request.project.as_ref(),
                    request.document_type,
                )?;
                if document.status != DocumentStatus::Active {
                    return Err(ApplyError::OperationForbidden {
                        document_id: *document_id,
                        detail: "attachments require an active document",
                    });
                }
                let relative_path = document
                    .relative_path
                    .parent()
                    .unwrap_or_else(|| Path::new(""))
                    .join(name.as_str());
                occupancy.reserve(&relative_path)?;
                plan.push(PlannedMutation::WriteNew {
                    relative_path,
                    bytes: read_payload(package_root, package, source.as_str())?,
                });
            }
        }
    }

    Ok(plan)
}

fn resolve_document<'a>(
    documents: &'a mut HashMap<DocumentId, VirtualDocument>,
    index: &ContentIndex,
    content_root: &Path,
    policy: ContentPolicy,
    document_id: DocumentId,
) -> Result<&'a mut VirtualDocument, ApplyError> {
    match documents.entry(document_id) {
        Entry::Occupied(entry) => Ok(entry.into_mut()),
        Entry::Vacant(entry) => {
            let record = index
                .get(document_id)
                .ok_or(ApplyError::DocumentNotFound { document_id })?;
            let markdown = read_canonical_markdown(
                content_root,
                record.relative_path(),
                policy.maximum_markdown_bytes,
                record.revision(),
            )?;
            Ok(entry.insert(VirtualDocument {
                relative_path: record.relative_path().to_path_buf(),
                document_type: record.location().document_type(),
                project: record.location().project().cloned(),
                created: record.metadata().created,
                updated: record.metadata().updated,
                archived: record.location().is_archived(),
                status: record.metadata().status,
                revision: record.revision(),
                markdown,
            }))
        }
    }
}

fn read_canonical_markdown(
    content_root: &Path,
    relative_path: &Path,
    maximum_bytes: u64,
    expected_revision: Revision,
) -> Result<Vec<u8>, ApplyError> {
    let mut bytes =
        Vec::with_capacity(usize::try_from(maximum_bytes.min(64 * 1024)).unwrap_or(64 * 1024));
    File::open(content_root.join(relative_path))
        .and_then(|file| {
            file.take(maximum_bytes.saturating_add(1))
                .read_to_end(&mut bytes)
        })
        .map_err(ApplyError::Io)?;
    if bytes.len() as u64 > maximum_bytes || revision(&bytes) != expected_revision {
        return Err(ApplyError::ContentChangedDuringApply);
    }
    Ok(bytes)
}

fn require_revision(
    document: &VirtualDocument,
    document_id: DocumentId,
    expected: Revision,
) -> Result<(), ApplyError> {
    if document.revision != expected {
        return Err(ApplyError::RevisionConflict {
            document_id,
            expected,
            actual: document.revision,
        });
    }
    Ok(())
}

fn require_accessible(
    document: &VirtualDocument,
    document_id: DocumentId,
    request_project: Option<&ProjectId>,
    request_type: DocumentType,
) -> Result<(), ApplyError> {
    if document.archived {
        return Err(ApplyError::OperationForbidden {
            document_id,
            detail: "archived documents cannot be modified",
        });
    }
    require_classification(document, document_id, request_project, request_type)
}

fn require_classification(
    document: &VirtualDocument,
    document_id: DocumentId,
    request_project: Option<&ProjectId>,
    request_type: DocumentType,
) -> Result<(), ApplyError> {
    if document.document_type != request_type || document.project.as_ref() != request_project {
        return Err(ApplyError::OperationForbidden {
            document_id,
            detail: "request classification differs from canonical content",
        });
    }
    Ok(())
}

fn require_mutable(
    document: &VirtualDocument,
    document_id: DocumentId,
    request_project: Option<&ProjectId>,
    request_type: DocumentType,
) -> Result<(), ApplyError> {
    require_accessible(document, document_id, request_project, request_type)?;
    if document.document_type == DocumentType::Log {
        return Err(ApplyError::OperationForbidden {
            document_id,
            detail: "logs are append-only",
        });
    }
    if document.status != DocumentStatus::Active {
        return Err(ApplyError::OperationForbidden {
            document_id,
            detail: "only active documents are mutable",
        });
    }
    Ok(())
}

fn require_movable(document: &VirtualDocument, document_id: DocumentId) -> Result<(), ApplyError> {
    if document.archived {
        return Err(ApplyError::OperationForbidden {
            document_id,
            detail: "document is already archived",
        });
    }
    if matches!(
        document.document_type,
        DocumentType::Log | DocumentType::Index
    ) {
        return Err(ApplyError::OperationForbidden {
            document_id,
            detail: "logs and index documents cannot be moved or archived",
        });
    }
    if document.status != DocumentStatus::Active {
        return Err(ApplyError::OperationForbidden {
            document_id,
            detail: "only active documents can be moved or archived",
        });
    }
    Ok(())
}

fn decode_payload_metadata(
    bytes: &[u8],
    policy: ContentPolicy,
    payload_path: &str,
) -> Result<DocumentMetadata, ApplyError> {
    let metadata =
        decode_document_metadata(bytes, policy.maximum_front_matter_bytes).map_err(|source| {
            ApplyError::InvalidPayloadDocument {
                path: payload_path.into(),
                source,
            }
        })?;
    metadata
        .validate_common(policy.document)
        .map_err(|source| ApplyError::InvalidPayloadMetadata {
            path: payload_path.into(),
            source,
        })?;
    Ok(metadata)
}

fn archived_markdown(
    bytes: &[u8],
    policy: ContentPolicy,
    request_id: agent_knowledge_core::RequestId,
    archived_at: OffsetDateTime,
) -> Result<Vec<u8>, ApplyError> {
    if bytes.len() as u64 > policy.maximum_markdown_bytes {
        return Err(ApplyError::ContentChangedDuringApply);
    }
    let mut metadata = decode_document_metadata(bytes, policy.maximum_front_matter_bytes)
        .map_err(|_| ApplyError::ContentChangedDuringApply)?;
    let previous_update = metadata.updated.unwrap_or(metadata.created);
    if archived_at <= previous_update {
        return Err(ApplyError::OperationForbidden {
            document_id: metadata.document_id,
            detail: "archive time must follow the previous document update",
        });
    }
    metadata.updated = Some(archived_at);
    metadata.request_id = request_id;
    metadata.status = DocumentStatus::Archived;
    let body = markdown_body(bytes).ok_or(ApplyError::ContentChangedDuringApply)?;
    let yaml = serde_saphyr::to_string(&metadata).map_err(ApplyError::MetadataEncoding)?;
    let mut archived = Vec::with_capacity(yaml.len() + body.len() + 9);
    archived.extend_from_slice(b"---\n");
    archived.extend_from_slice(yaml.as_bytes());
    if !yaml.ends_with('\n') {
        archived.push(b'\n');
    }
    archived.extend_from_slice(b"---\n");
    archived.extend_from_slice(body);
    Ok(archived)
}

fn markdown_body(markdown: &[u8]) -> Option<&[u8]> {
    let text = std::str::from_utf8(markdown).ok()?;
    let opening_length = if text.starts_with("---\r\n") {
        5
    } else if text.starts_with("---\n") {
        4
    } else {
        return None;
    };
    let remainder = &text[opening_length..];
    let mut offset = 0_usize;
    for line in remainder.split_inclusive('\n') {
        let content = line.strip_suffix('\n').unwrap_or(line);
        let content = content.strip_suffix('\r').unwrap_or(content);
        offset += line.len();
        if content == "---" {
            return Some(&remainder.as_bytes()[offset..]);
        }
    }
    None
}

fn create_document_path(
    project: Option<&ProjectId>,
    document_type: DocumentType,
    created: OffsetDateTime,
    document_id: DocumentId,
) -> PathBuf {
    if document_type == DocumentType::Index {
        return project.map_or_else(
            || PathBuf::from("index.md"),
            |project| {
                PathBuf::from("projects")
                    .join(project.as_str())
                    .join("index.md")
            },
        );
    }

    let category = category_name(document_type);
    let mut path = project.map_or_else(
        || PathBuf::from("inbox").join(category),
        |project| {
            PathBuf::from("projects")
                .join(project.as_str())
                .join(category)
        },
    );
    if document_type == DocumentType::Log {
        path.push(format!("{:04}", created.year()));
        path.push(format!("{:02}", u8::from(created.month())));
        path.push(format!("{:02}", created.day()));
        path.push(format!(
            "{:02}{:02}{:02}-{}",
            created.hour(),
            created.minute(),
            created.second(),
            document_id
        ));
    } else {
        path.push(format!(
            "{:04}-{:02}-{:02}-{}",
            created.year(),
            u8::from(created.month()),
            created.day(),
            document_id
        ));
    }
    path.join("index.md")
}

fn archive_document_path(document: &VirtualDocument, document_id: DocumentId) -> PathBuf {
    let category = category_name(document.document_type);
    let bundle_name = document
        .relative_path
        .parent()
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        .map_or_else(|| document_id.to_string(), str::to_owned);
    if let Some(project) = document.project.as_ref() {
        PathBuf::from("projects")
            .join(project.as_str())
            .join("archive")
            .join(category)
            .join(bundle_name)
            .join("index.md")
    } else {
        PathBuf::from("archive")
            .join(category)
            .join(bundle_name)
            .join("index.md")
    }
}

fn bundle_path(document_path: &Path, document_type: DocumentType) -> PathBuf {
    if document_type == DocumentType::Index {
        document_path.to_path_buf()
    } else {
        document_path
            .parent()
            .unwrap_or(document_path)
            .to_path_buf()
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

struct VirtualOccupancy<'a> {
    content_root: &'a Path,
    entries: HashMap<PathBuf, bool>,
}

impl<'a> VirtualOccupancy<'a> {
    fn new(content_root: &'a Path) -> Self {
        Self {
            content_root,
            entries: HashMap::new(),
        }
    }

    fn reserve(&mut self, relative_path: &Path) -> Result<(), ApplyError> {
        if self.is_occupied(relative_path)? {
            return Err(ApplyError::DestinationExists {
                path: relative_path.to_path_buf(),
            });
        }
        self.entries.insert(relative_path.to_path_buf(), true);
        Ok(())
    }

    fn move_path(&mut self, source: &Path, destination: &Path) -> Result<(), ApplyError> {
        if source == destination {
            return Err(ApplyError::DestinationExists {
                path: destination.to_path_buf(),
            });
        }
        self.reserve(destination)?;
        self.entries.insert(source.to_path_buf(), false);
        Ok(())
    }

    fn is_occupied(&mut self, relative_path: &Path) -> Result<bool, ApplyError> {
        if let Some(occupied) = self.entries.get(relative_path) {
            return Ok(*occupied);
        }
        match fs::symlink_metadata(self.content_root.join(relative_path)) {
            Ok(_) => {
                self.entries.insert(relative_path.to_path_buf(), true);
                Ok(true)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.entries.insert(relative_path.to_path_buf(), false);
                Ok(false)
            }
            Err(error) => Err(ApplyError::Io(error)),
        }
    }
}

fn read_payload(
    package_root: &Path,
    package: &ValidatedPackage,
    relative_path: &str,
) -> Result<Vec<u8>, ApplyError> {
    let metadata = package
        .payload()
        .iter()
        .find(|metadata| metadata.path().as_str() == relative_path)
        .ok_or(ApplyError::ClaimPackageChanged)?;
    let mut bytes = Vec::with_capacity(
        usize::try_from(metadata.byte_length().min(64 * 1024)).unwrap_or(64 * 1024),
    );
    File::open(package_root.join("payload").join(relative_path))
        .and_then(|file| {
            file.take(metadata.byte_length().saturating_add(1))
                .read_to_end(&mut bytes)
        })
        .map_err(ApplyError::Io)?;
    if bytes.len() as u64 != metadata.byte_length() || revision(&bytes) != metadata.revision() {
        return Err(ApplyError::ClaimPackageChanged);
    }
    Ok(bytes)
}

fn revision(bytes: &[u8]) -> Revision {
    Revision::from_bytes(Sha256::digest(bytes).into())
}

fn execute_plan(
    content_root: &Path,
    plan: Vec<PlannedMutation>,
) -> Result<Vec<AppliedMove>, ApplyError> {
    let mut moves = Vec::new();
    for mutation in plan {
        match mutation {
            PlannedMutation::WriteNew {
                relative_path,
                bytes,
            } => {
                let path = content_root.join(&relative_path);
                create_parent(&path)?;
                let mut file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(path)
                    .map_err(ApplyError::Io)?;
                file.write_all(&bytes).map_err(ApplyError::Io)?;
            }
            PlannedMutation::Replace {
                relative_path,
                bytes,
            } => {
                fs::write(content_root.join(relative_path), bytes).map_err(ApplyError::Io)?;
            }
            PlannedMutation::Move {
                source,
                destination,
            } => {
                moves.push(AppliedMove {
                    source: source.clone(),
                    destination: destination.clone(),
                });
                let destination_path = content_root.join(&destination);
                create_parent(&destination_path)?;
                fs::rename(content_root.join(source), destination_path).map_err(ApplyError::Io)?;
            }
        }
    }
    Ok(moves)
}

fn create_parent(path: &Path) -> Result<(), ApplyError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(ApplyError::Io)?;
    }
    Ok(())
}

/// A deterministic request-application or transient filesystem failure.
#[derive(Debug)]
pub enum ApplyError {
    /// The client request time was later than its durable queue acceptance.
    RequestCreatedAfterAcceptance,
    /// The base content tree was invalid.
    ContentIndex(ContentIndexError),
    /// The resulting content tree was invalid.
    ResultingContent(ContentIndexError),
    /// A requested document did not exist.
    DocumentNotFound {
        /// Missing identity.
        document_id: DocumentId,
    },
    /// A created document identity already existed.
    DocumentIdConflict {
        /// Conflicting identity.
        document_id: DocumentId,
    },
    /// An optimistic revision precondition failed.
    RevisionConflict {
        /// Conflicting identity.
        document_id: DocumentId,
        /// Client-supplied revision.
        expected: Revision,
        /// Current content revision.
        actual: Revision,
    },
    /// An operation violated document type or lifecycle policy.
    OperationForbidden {
        /// Affected identity.
        document_id: DocumentId,
        /// Stable implementation detail.
        detail: &'static str,
    },
    /// A derived document or attachment destination already existed.
    DestinationExists {
        /// Conflicting relative content path.
        path: PathBuf,
    },
    /// A referenced Markdown payload could not be decoded.
    InvalidPayloadDocument {
        /// Payload-relative path.
        path: PathBuf,
        /// Decode failure.
        source: DocumentParseError,
    },
    /// A referenced Markdown payload had invalid shared metadata.
    InvalidPayloadMetadata {
        /// Payload-relative path.
        path: PathBuf,
        /// Validation failure.
        source: DocumentValidationError,
    },
    /// The accepted package no longer matched its claim-time validation.
    ClaimPackageChanged,
    /// Canonical content changed after the base index was built.
    ContentChangedDuringApply,
    /// Worker-generated front matter could not be encoded.
    MetadataEncoding(serde_saphyr::ser::Error),
    /// Live accepted-package revalidation failed.
    PackageValidation(PackageValidationError),
    /// A filesystem operation failed.
    Io(io::Error),
}

impl fmt::Display for ApplyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RequestCreatedAfterAcceptance => {
                formatter.write_str("request creation time cannot follow queue acceptance")
            }
            Self::ContentIndex(error) => write!(formatter, "base content is invalid: {error}"),
            Self::ResultingContent(error) => {
                write!(formatter, "resulting content is invalid: {error}")
            }
            Self::DocumentNotFound { document_id } => {
                write!(formatter, "document `{document_id}` was not found")
            }
            Self::DocumentIdConflict { document_id } => {
                write!(formatter, "document ID `{document_id}` already exists")
            }
            Self::RevisionConflict {
                document_id,
                expected,
                actual,
            } => write!(
                formatter,
                "document `{document_id}` revision conflict: expected `{expected}`, found `{actual}`"
            ),
            Self::OperationForbidden {
                document_id,
                detail,
            } => write!(
                formatter,
                "operation on document `{document_id}` is forbidden: {detail}"
            ),
            Self::DestinationExists { path } => {
                write!(formatter, "destination `{}` already exists", path.display())
            }
            Self::InvalidPayloadDocument { path, source } => {
                write!(
                    formatter,
                    "payload `{}` is invalid: {source}",
                    path.display()
                )
            }
            Self::InvalidPayloadMetadata { path, source } => write!(
                formatter,
                "payload `{}` metadata is invalid: {source}",
                path.display()
            ),
            Self::ClaimPackageChanged => {
                formatter.write_str("claimed package changed after claim-time validation")
            }
            Self::ContentChangedDuringApply => {
                formatter.write_str("canonical content changed while applying a request")
            }
            Self::MetadataEncoding(error) => {
                write!(
                    formatter,
                    "generated document metadata could not be encoded: {error}"
                )
            }
            Self::PackageValidation(error) => {
                write!(formatter, "claimed package revalidation failed: {error}")
            }
            Self::Io(error) => write!(formatter, "content mutation I/O failed: {error}"),
        }
    }
}

impl std::error::Error for ApplyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ContentIndex(error) | Self::ResultingContent(error) => Some(error),
            Self::InvalidPayloadDocument { source, .. } => Some(source),
            Self::InvalidPayloadMetadata { source, .. } => Some(source),
            Self::PackageValidation(error) => Some(error),
            Self::MetadataEncoding(error) => Some(error),
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl ApplyError {
    pub(super) const fn request_error_code(&self) -> Option<ErrorCode> {
        match self {
            Self::RequestCreatedAfterAcceptance => Some(ErrorCode::InvalidRequest),
            Self::DocumentNotFound { .. } => Some(ErrorCode::DocumentNotFound),
            Self::DocumentIdConflict { .. } => Some(ErrorCode::DocumentIdConflict),
            Self::RevisionConflict { .. } => Some(ErrorCode::RevisionConflict),
            Self::OperationForbidden { .. } | Self::DestinationExists { .. } => {
                Some(ErrorCode::OperationForbidden)
            }
            Self::ResultingContent(ContentIndexError::Io(_)) => None,
            Self::ResultingContent(_) => Some(ErrorCode::ContentValidationFailed),
            Self::InvalidPayloadDocument { .. } | Self::InvalidPayloadMetadata { .. } => {
                Some(ErrorCode::InvalidFrontMatter)
            }
            Self::ContentIndex(_)
            | Self::ClaimPackageChanged
            | Self::ContentChangedDuringApply
            | Self::MetadataEncoding(_)
            | Self::PackageValidation(_)
            | Self::Io(_) => None,
        }
    }
}

#[cfg(test)]
mod tests;

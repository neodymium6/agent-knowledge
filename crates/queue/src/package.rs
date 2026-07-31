use std::collections::{BTreeSet, HashSet};
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Read};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use agent_knowledge_core::{
    ChangeRequest, DocumentLimits, ErrorCode, Operation, PayloadPath, RequestDecodeError,
    RequestLimits, RequestValidationError, Revision, RevisionParseError,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;

mod markdown;
pub use markdown::MarkdownValidationError;

const REQUEST_FILE_NAME: &str = "request.json";
const PAYLOAD_DIRECTORY_NAME: &str = "payload";
const DIGEST_FILE_NAME: &str = "digest";
const ACCEPTANCE_FILE_NAME: &str = "acceptance.json";
const PHASE_FILE_NAME: &str = "phase.json";
const RESULT_FILE_NAME: &str = "result.json";
const DIGEST_DOMAIN: &[u8] = b"agent-knowledge-request-package-v1\0";
const HASH_BUFFER_LENGTH: usize = 64 * 1024;
const MAXIMUM_DIGEST_FILE_BYTES: u64 = 72;
const MAXIMUM_ACCEPTANCE_FILE_BYTES: u64 = 256;

/// Immutable Gateway-owned ordering metadata for an accepted package.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptanceMetadata {
    /// Queue-local durable acceptance order, starting at one.
    pub sequence: NonZeroU64,
    /// Central-server timestamp recorded while holding the acceptance lock.
    #[serde(with = "time::serde::rfc3339")]
    pub accepted_at: OffsetDateTime,
}

/// Configurable byte and file-count limits for one request package.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PackageLimits {
    /// Maximum combined bytes in `request.json` and all payload files.
    pub maximum_total_bytes: u64,
    /// Maximum bytes in any individual file, including `request.json`.
    pub maximum_file_bytes: u64,
    /// Maximum number of files, including `request.json`.
    pub maximum_file_count: usize,
    /// Maximum number of nested directories below `payload/`.
    pub maximum_directory_count: usize,
    /// Maximum combined request file, payload files, and payload directories.
    pub maximum_entry_count: usize,
    /// Maximum path components in one payload file or directory.
    pub maximum_path_components: usize,
    /// Maximum bytes in one Markdown document's YAML front matter.
    pub maximum_front_matter_bytes: usize,
    /// Validation limits for the decoded change request.
    pub request: RequestLimits,
    /// Validation limits for Markdown document front matter.
    pub document: DocumentLimits,
}

impl Default for PackageLimits {
    fn default() -> Self {
        Self {
            maximum_total_bytes: 64 * 1024 * 1024,
            maximum_file_bytes: 32 * 1024 * 1024,
            maximum_file_count: 256,
            maximum_directory_count: 1_024,
            maximum_entry_count: 1_280,
            maximum_path_components: 64,
            maximum_front_matter_bytes: 64 * 1024,
            request: RequestLimits::default(),
            document: DocumentLimits::default(),
        }
    }
}

/// Package validation policy supplied by application configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackagePolicy {
    limits: PackageLimits,
    allowed_attachment_extensions: BTreeSet<String>,
}

impl PackagePolicy {
    /// Creates a policy from configured limits and extension names without dots.
    ///
    /// # Errors
    ///
    /// Returns an error when an extension is empty or is not lowercase ASCII.
    pub fn new<I, S>(
        limits: PackageLimits,
        allowed_attachment_extensions: I,
    ) -> Result<Self, PackagePolicyError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut extensions = BTreeSet::new();
        for extension in allowed_attachment_extensions {
            let extension = extension.as_ref();
            if extension.is_empty()
                || !extension
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
            {
                return Err(PackagePolicyError::InvalidAttachmentExtension(
                    extension.into(),
                ));
            }
            extensions.insert(extension.into());
        }

        Ok(Self {
            limits,
            allowed_attachment_extensions: extensions,
        })
    }

    /// Returns the configured package limits.
    #[must_use]
    pub const fn limits(&self) -> PackageLimits {
        self.limits
    }

    fn allows_attachment(&self, name: &str) -> bool {
        Path::new(name)
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| self.allowed_attachment_extensions.contains(extension))
    }
}

impl Default for PackagePolicy {
    fn default() -> Self {
        match Self::new(
            PackageLimits::default(),
            ["png", "jpg", "jpeg", "svg", "csv", "json", "pdf", "html"],
        ) {
            Ok(policy) => policy,
            Err(error) => panic!("built-in package policy must be valid: {error}"),
        }
    }
}

/// An invalid package-policy configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PackagePolicyError {
    /// An attachment extension was not a lowercase ASCII name.
    InvalidAttachmentExtension(String),
}

impl fmt::Display for PackagePolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidAttachmentExtension(extension) => write!(
                formatter,
                "attachment extension `{extension}` must contain lowercase ASCII letters or digits"
            ),
        }
    }
}

impl std::error::Error for PackagePolicyError {}

/// A deterministic digest of normalized request metadata and payload bytes.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PackageDigest(Revision);

impl PackageDigest {
    /// Returns the digest using the shared SHA-256 representation.
    #[must_use]
    pub const fn as_revision(&self) -> Revision {
        self.0
    }
}

impl fmt::Display for PackageDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for PackageDigest {
    type Err = RevisionParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse().map(Self)
    }
}

/// Metadata for one validated payload file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PayloadMetadata {
    path: PayloadPath,
    byte_length: u64,
}

impl PayloadMetadata {
    /// Returns the normalized path relative to `payload/`.
    #[must_use]
    pub const fn path(&self) -> &PayloadPath {
        &self.path
    }

    /// Returns the exact file length included in the package digest.
    #[must_use]
    pub const fn byte_length(&self) -> u64 {
        self.byte_length
    }
}

/// A package whose layout, request, payload references, limits, and digest are valid.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedPackage {
    request: ChangeRequest,
    digest: PackageDigest,
    payload: Vec<PayloadMetadata>,
    acceptance: Option<AcceptanceMetadata>,
}

impl ValidatedPackage {
    /// Returns the decoded change request.
    #[must_use]
    pub const fn request(&self) -> &ChangeRequest {
        &self.request
    }

    /// Returns the normalized package digest.
    #[must_use]
    pub const fn digest(&self) -> PackageDigest {
        self.digest
    }

    /// Returns payload metadata in normalized path order.
    #[must_use]
    pub fn payload(&self) -> &[PayloadMetadata] {
        &self.payload
    }

    /// Returns Gateway-owned ordering metadata for an accepted package.
    ///
    /// Incoming packages do not have acceptance metadata yet.
    #[must_use]
    pub const fn acceptance(&self) -> Option<AcceptanceMetadata> {
        self.acceptance
    }
}

/// Validates an extracted, unaccepted request-package directory.
///
/// The directory must contain exactly `request.json` and `payload/`. Payload
/// entries may only be regular files and the directories required to contain
/// them.
///
/// # Errors
///
/// Returns the first deterministic validation failure or an I/O error.
pub fn validate_package(
    package_root: &Path,
    policy: &PackagePolicy,
) -> Result<ValidatedPackage, PackageValidationError> {
    validate_package_root(package_root, false)?;
    validate_package_contents(package_root, policy)
}

/// Revalidates an accepted package and its stored digest.
///
/// # Errors
///
/// Returns an error when the immutable package is malformed, its contents no
/// longer match its stored digest, or an I/O operation fails.
pub fn validate_accepted_package(
    package_root: &Path,
    policy: &PackagePolicy,
) -> Result<ValidatedPackage, PackageValidationError> {
    validate_package_root(package_root, true)?;
    let stored_digest = read_digest_file(&package_root.join(DIGEST_FILE_NAME))?;
    let acceptance = read_acceptance_file(&package_root.join(ACCEPTANCE_FILE_NAME))?;
    let mut package = validate_package_contents(package_root, policy)?;
    if stored_digest != package.digest {
        return Err(PackageValidationError::StoredDigestMismatch {
            stored: stored_digest,
            calculated: package.digest,
        });
    }
    package.acceptance = Some(acceptance);
    Ok(package)
}

fn validate_package_contents(
    package_root: &Path,
    policy: &PackagePolicy,
) -> Result<ValidatedPackage, PackageValidationError> {
    let request_path = package_root.join(REQUEST_FILE_NAME);
    let request_bytes = read_limited_file(&request_path, policy.limits.maximum_file_bytes)?;
    let mut total_bytes = request_bytes.len() as u64;
    let mut file_count = 1_usize;
    enforce_totals(total_bytes, file_count, policy.limits)?;
    enforce_entry_count(file_count, policy.limits)?;

    let request = ChangeRequest::decode_json(&request_bytes)
        .map_err(PackageValidationError::InvalidRequest)?;
    request
        .validate(policy.limits.request)
        .map_err(PackageValidationError::InvalidRequestMetadata)?;

    let payload_root = package_root.join(PAYLOAD_DIRECTORY_NAME);
    let mut payload_files = Vec::new();
    scan_payload_directory(
        &payload_root,
        &payload_root,
        policy.limits,
        &mut total_bytes,
        &mut file_count,
        &mut payload_files,
    )?;
    payload_files.sort_by(|left, right| left.path.as_str().cmp(right.path.as_str()));

    validate_payload_references(&request, &payload_files, policy)?;
    markdown::validate_documents(
        &request,
        &payload_root,
        policy.limits.document,
        policy.limits.maximum_front_matter_bytes,
    )
    .map_err(PackageValidationError::InvalidFrontMatter)?;

    let canonical_request =
        serde_json::to_vec(&request).map_err(PackageValidationError::CanonicalRequestJson)?;
    let digest = calculate_digest(&canonical_request, &payload_root, &payload_files)?;

    Ok(ValidatedPackage {
        request,
        digest,
        payload: payload_files,
        acceptance: None,
    })
}

fn validate_package_root(
    package_root: &Path,
    accepted: bool,
) -> Result<(), PackageValidationError> {
    let root_metadata = fs::symlink_metadata(package_root).map_err(PackageValidationError::Io)?;
    if !root_metadata.file_type().is_dir() {
        return Err(PackageValidationError::InvalidEntryType {
            path: package_root.into(),
        });
    }

    let mut names = HashSet::new();
    for entry in fs::read_dir(package_root).map_err(PackageValidationError::Io)? {
        let entry = entry.map_err(PackageValidationError::Io)?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| PackageValidationError::InvalidLayout)?;
        if name != REQUEST_FILE_NAME
            && name != PAYLOAD_DIRECTORY_NAME
            && !(accepted && name == DIGEST_FILE_NAME)
            && !(accepted && name == ACCEPTANCE_FILE_NAME)
            && !(accepted && name == PHASE_FILE_NAME)
            && !(accepted && name == RESULT_FILE_NAME)
        {
            return Err(PackageValidationError::UnexpectedTopLevelEntry(name));
        }
        names.insert(name);
    }

    if !names.contains(REQUEST_FILE_NAME) {
        return Err(PackageValidationError::MissingTopLevelEntry(
            REQUEST_FILE_NAME,
        ));
    }
    if !names.contains(PAYLOAD_DIRECTORY_NAME) {
        return Err(PackageValidationError::MissingTopLevelEntry(
            PAYLOAD_DIRECTORY_NAME,
        ));
    }
    if accepted && !names.contains(DIGEST_FILE_NAME) {
        return Err(PackageValidationError::MissingTopLevelEntry(
            DIGEST_FILE_NAME,
        ));
    }
    if accepted && !names.contains(ACCEPTANCE_FILE_NAME) {
        return Err(PackageValidationError::MissingTopLevelEntry(
            ACCEPTANCE_FILE_NAME,
        ));
    }

    let request_metadata = fs::symlink_metadata(package_root.join(REQUEST_FILE_NAME))
        .map_err(PackageValidationError::Io)?;
    validate_regular_file(&request_metadata, Path::new(REQUEST_FILE_NAME))?;

    let payload_metadata = fs::symlink_metadata(package_root.join(PAYLOAD_DIRECTORY_NAME))
        .map_err(PackageValidationError::Io)?;
    if !payload_metadata.file_type().is_dir() {
        return Err(PackageValidationError::InvalidEntryType {
            path: PathBuf::from(PAYLOAD_DIRECTORY_NAME),
        });
    }
    if accepted {
        let digest_metadata = fs::symlink_metadata(package_root.join(DIGEST_FILE_NAME))
            .map_err(PackageValidationError::Io)?;
        validate_regular_file(&digest_metadata, Path::new(DIGEST_FILE_NAME))?;
        let acceptance_metadata = fs::symlink_metadata(package_root.join(ACCEPTANCE_FILE_NAME))
            .map_err(PackageValidationError::Io)?;
        validate_regular_file(&acceptance_metadata, Path::new(ACCEPTANCE_FILE_NAME))?;
        for sidecar in [PHASE_FILE_NAME, RESULT_FILE_NAME] {
            if names.contains(sidecar) {
                let metadata = fs::symlink_metadata(package_root.join(sidecar))
                    .map_err(PackageValidationError::Io)?;
                validate_regular_file(&metadata, Path::new(sidecar))?;
            }
        }
    }

    Ok(())
}

fn read_acceptance_file(path: &Path) -> Result<AcceptanceMetadata, PackageValidationError> {
    let bytes = read_limited_file(path, MAXIMUM_ACCEPTANCE_FILE_BYTES)?;
    serde_json::from_slice(&bytes).map_err(PackageValidationError::InvalidAcceptanceMetadata)
}

fn read_digest_file(path: &Path) -> Result<PackageDigest, PackageValidationError> {
    let mut bytes = Vec::with_capacity(MAXIMUM_DIGEST_FILE_BYTES as usize);
    File::open(path)
        .map_err(PackageValidationError::Io)?
        .take(MAXIMUM_DIGEST_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(PackageValidationError::Io)?;
    if bytes.len() as u64 > MAXIMUM_DIGEST_FILE_BYTES {
        return Err(PackageValidationError::InvalidStoredDigest);
    }
    let contents =
        std::str::from_utf8(&bytes).map_err(|_| PackageValidationError::InvalidStoredDigest)?;
    let Some(value) = contents.strip_suffix('\n') else {
        return Err(PackageValidationError::InvalidStoredDigest);
    };
    if value.contains('\n') {
        return Err(PackageValidationError::InvalidStoredDigest);
    }
    value
        .parse()
        .map_err(|_| PackageValidationError::InvalidStoredDigest)
}

fn scan_payload_directory(
    directory: &Path,
    payload_root: &Path,
    limits: PackageLimits,
    total_bytes: &mut u64,
    file_count: &mut usize,
    files: &mut Vec<PayloadMetadata>,
) -> Result<usize, PackageValidationError> {
    let mut stack = vec![directory.to_path_buf()];
    let mut directories = Vec::new();
    let mut nonempty_directories = HashSet::new();
    let mut entry_count = *file_count;

    while let Some(current) = stack.pop() {
        let remaining = limits.maximum_entry_count.saturating_sub(entry_count);
        let mut entries = fs::read_dir(&current)
            .map_err(PackageValidationError::Io)?
            .take(remaining.saturating_add(1))
            .collect::<Result<Vec<_>, _>>()
            .map_err(PackageValidationError::Io)?;
        if entries.len() > remaining {
            return Err(PackageValidationError::LimitExceeded {
                limit: PackageLimit::EntryCount,
                maximum: limits.maximum_entry_count as u64,
                actual: entry_count.saturating_add(entries.len()) as u64,
            });
        }
        entry_count += entries.len();
        entries.sort_by_key(fs::DirEntry::file_name);

        for entry in entries {
            let entry_path = entry.path();
            let metadata = fs::symlink_metadata(&entry_path).map_err(PackageValidationError::Io)?;
            let relative_path = entry_path
                .strip_prefix(payload_root)
                .map_err(|_| PackageValidationError::InvalidLayout)?;
            let relative = relative_path
                .to_str()
                .ok_or(PackageValidationError::InvalidLayout)?
                .to_owned();
            let path = relative
                .parse::<PayloadPath>()
                .map_err(|_| PackageValidationError::InvalidPayloadPath(relative.clone()))?;
            enforce_path_components(&path, limits)?;

            if metadata.file_type().is_dir() {
                let actual = directories.len().checked_add(1).ok_or(
                    PackageValidationError::LimitExceeded {
                        limit: PackageLimit::DirectoryCount,
                        maximum: limits.maximum_directory_count as u64,
                        actual: u64::MAX,
                    },
                )?;
                if actual > limits.maximum_directory_count {
                    return Err(PackageValidationError::LimitExceeded {
                        limit: PackageLimit::DirectoryCount,
                        maximum: limits.maximum_directory_count as u64,
                        actual: actual as u64,
                    });
                }
                directories.push((relative_path.to_path_buf(), path));
                stack.push(entry_path);
                continue;
            }

            validate_regular_file(
                &metadata,
                &PathBuf::from(PAYLOAD_DIRECTORY_NAME).join(&relative),
            )?;
            enforce_file_size(metadata.len(), limits.maximum_file_bytes, path.as_str())?;
            *total_bytes = total_bytes.checked_add(metadata.len()).ok_or(
                PackageValidationError::LimitExceeded {
                    limit: PackageLimit::TotalBytes,
                    maximum: limits.maximum_total_bytes,
                    actual: u64::MAX,
                },
            )?;
            *file_count =
                file_count
                    .checked_add(1)
                    .ok_or(PackageValidationError::LimitExceeded {
                        limit: PackageLimit::FileCount,
                        maximum: limits.maximum_file_count as u64,
                        actual: u64::MAX,
                    })?;
            enforce_totals(*total_bytes, *file_count, limits)?;

            let mut parent = relative_path.parent();
            while let Some(directory) = parent {
                if directory.as_os_str().is_empty() {
                    break;
                }
                nonempty_directories.insert(directory.to_path_buf());
                parent = directory.parent();
            }
            files.push(PayloadMetadata {
                path,
                byte_length: metadata.len(),
            });
        }
    }

    directories.sort_by(|left, right| left.0.cmp(&right.0));
    if let Some((_, path)) = directories
        .iter()
        .find(|(relative, _)| !nonempty_directories.contains(relative))
    {
        return Err(PackageValidationError::EmptyPayloadDirectory(path.clone()));
    }
    Ok(files.len())
}

fn enforce_path_components(
    path: &PayloadPath,
    limits: PackageLimits,
) -> Result<(), PackageValidationError> {
    let actual = path.as_str().split('/').count();
    if actual > limits.maximum_path_components {
        return Err(PackageValidationError::LimitExceeded {
            limit: PackageLimit::PathComponents,
            maximum: limits.maximum_path_components as u64,
            actual: actual as u64,
        });
    }
    Ok(())
}

fn validate_regular_file(
    metadata: &fs::Metadata,
    relative_path: &Path,
) -> Result<(), PackageValidationError> {
    if !metadata.file_type().is_file() {
        return Err(PackageValidationError::InvalidEntryType {
            path: relative_path.into(),
        });
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        if metadata.nlink() > 1 {
            return Err(PackageValidationError::HardLinkedFile {
                path: relative_path.into(),
            });
        }
        if metadata.mode() & 0o111 != 0 {
            return Err(PackageValidationError::ExecutableFile {
                path: relative_path.into(),
            });
        }
    }

    Ok(())
}

fn validate_payload_references(
    request: &ChangeRequest,
    payload: &[PayloadMetadata],
    policy: &PackagePolicy,
) -> Result<(), PackageValidationError> {
    let available = payload
        .iter()
        .map(|file| file.path.as_str())
        .collect::<HashSet<_>>();
    let mut referenced = HashSet::new();

    for operation in &request.operations {
        match operation {
            Operation::CreateDocument { content, .. }
            | Operation::UpdateDocument { content, .. } => {
                if Path::new(content.as_str())
                    .extension()
                    .and_then(|value| value.to_str())
                    != Some("md")
                {
                    return Err(PackageValidationError::MarkdownExtensionRequired(
                        content.clone(),
                    ));
                }
                require_payload(content, &available)?;
                referenced.insert(content.as_str());
            }
            Operation::AddAttachment { source, name, .. } => {
                if !policy.allows_attachment(name.as_str()) {
                    return Err(PackageValidationError::UnsupportedAttachment(
                        name.to_string(),
                    ));
                }
                require_payload(source, &available)?;
                referenced.insert(source.as_str());
            }
            Operation::MoveDocument { .. } | Operation::ArchiveDocument { .. } => {}
        }
    }

    if let Some(unexpected) = payload
        .iter()
        .find(|file| !referenced.contains(file.path.as_str()))
    {
        return Err(PackageValidationError::UnexpectedPayload(
            unexpected.path.clone(),
        ));
    }

    Ok(())
}

fn require_payload(
    path: &PayloadPath,
    available: &HashSet<&str>,
) -> Result<(), PackageValidationError> {
    if available.contains(path.as_str()) {
        Ok(())
    } else {
        Err(PackageValidationError::MissingPayload(path.clone()))
    }
}

fn read_limited_file(path: &Path, maximum: u64) -> Result<Vec<u8>, PackageValidationError> {
    let metadata = fs::symlink_metadata(path).map_err(PackageValidationError::Io)?;
    enforce_file_size(metadata.len(), maximum, &path.display().to_string())?;

    let capacity =
        usize::try_from(metadata.len()).map_err(|_| PackageValidationError::LimitExceeded {
            limit: PackageLimit::IndividualFileBytes,
            maximum,
            actual: metadata.len(),
        })?;
    let mut bytes = Vec::with_capacity(capacity);
    File::open(path)
        .map_err(PackageValidationError::Io)?
        .take(maximum.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(PackageValidationError::Io)?;
    if bytes.len() as u64 > maximum {
        return Err(PackageValidationError::LimitExceeded {
            limit: PackageLimit::IndividualFileBytes,
            maximum,
            actual: bytes.len() as u64,
        });
    }
    Ok(bytes)
}

fn enforce_file_size(actual: u64, maximum: u64, path: &str) -> Result<(), PackageValidationError> {
    if actual > maximum {
        return Err(PackageValidationError::FileTooLarge {
            path: path.into(),
            maximum,
            actual,
        });
    }
    Ok(())
}

fn enforce_totals(
    total_bytes: u64,
    file_count: usize,
    limits: PackageLimits,
) -> Result<(), PackageValidationError> {
    if total_bytes > limits.maximum_total_bytes {
        return Err(PackageValidationError::LimitExceeded {
            limit: PackageLimit::TotalBytes,
            maximum: limits.maximum_total_bytes,
            actual: total_bytes,
        });
    }
    if file_count > limits.maximum_file_count {
        return Err(PackageValidationError::LimitExceeded {
            limit: PackageLimit::FileCount,
            maximum: limits.maximum_file_count as u64,
            actual: file_count as u64,
        });
    }
    Ok(())
}

fn enforce_entry_count(
    entry_count: usize,
    limits: PackageLimits,
) -> Result<(), PackageValidationError> {
    if entry_count > limits.maximum_entry_count {
        return Err(PackageValidationError::LimitExceeded {
            limit: PackageLimit::EntryCount,
            maximum: limits.maximum_entry_count as u64,
            actual: entry_count as u64,
        });
    }
    Ok(())
}

fn calculate_digest(
    canonical_request: &[u8],
    payload_root: &Path,
    payload: &[PayloadMetadata],
) -> Result<PackageDigest, PackageValidationError> {
    let mut hasher = Sha256::new();
    hasher.update(DIGEST_DOMAIN);
    hash_bytes(&mut hasher, b"request", canonical_request);
    hasher.update((payload.len() as u64).to_be_bytes());

    let mut buffer = [0_u8; HASH_BUFFER_LENGTH];
    for file in payload {
        hash_length_prefixed(&mut hasher, file.path.as_str().as_bytes());
        hasher.update(file.byte_length.to_be_bytes());

        let mut source = File::open(payload_root.join(file.path.as_str()))
            .map_err(PackageValidationError::Io)?;
        let mut observed_length = 0_u64;
        loop {
            let read = source
                .read(&mut buffer)
                .map_err(PackageValidationError::Io)?;
            if read == 0 {
                break;
            }
            observed_length = observed_length.checked_add(read as u64).ok_or(
                PackageValidationError::FileChangedDuringValidation(file.path.clone()),
            )?;
            if observed_length > file.byte_length {
                return Err(PackageValidationError::FileChangedDuringValidation(
                    file.path.clone(),
                ));
            }
            hasher.update(&buffer[..read]);
        }
        if observed_length != file.byte_length {
            return Err(PackageValidationError::FileChangedDuringValidation(
                file.path.clone(),
            ));
        }
    }

    let bytes: [u8; 32] = hasher.finalize().into();
    Ok(PackageDigest(Revision::from_bytes(bytes)))
}

fn hash_bytes(hasher: &mut Sha256, label: &[u8], bytes: &[u8]) {
    hash_length_prefixed(hasher, label);
    hash_length_prefixed(hasher, bytes);
}

fn hash_length_prefixed(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

/// The configurable limit that rejected a package.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PackageLimit {
    /// Combined request and payload bytes.
    TotalBytes,
    /// Bytes in one file.
    IndividualFileBytes,
    /// Number of request and payload files.
    FileCount,
    /// Number of nested directories below `payload/`.
    DirectoryCount,
    /// Combined number of request, payload-file, and payload-directory entries.
    EntryCount,
    /// Number of components in one payload path.
    PathComponents,
}

/// A package validation failure.
#[derive(Debug)]
pub enum PackageValidationError {
    /// A file-system operation failed.
    Io(io::Error),
    /// The package root was not decodable as the required layout.
    InvalidLayout,
    /// A required top-level entry was absent.
    MissingTopLevelEntry(&'static str),
    /// An unexpected top-level entry was present.
    UnexpectedTopLevelEntry(String),
    /// An entry was a link, special file, or the wrong required type.
    InvalidEntryType {
        /// The package-relative entry path.
        path: PathBuf,
    },
    /// A payload path was not normalized or safe.
    InvalidPayloadPath(String),
    /// A nested payload directory contained no files.
    EmptyPayloadDirectory(PayloadPath),
    /// Request JSON decoding or typed path conversion failed.
    InvalidRequest(RequestDecodeError),
    /// Request-level deterministic validation failed.
    InvalidRequestMetadata(RequestValidationError),
    /// Serializing an already typed request for digest calculation failed.
    CanonicalRequestJson(serde_json::Error),
    /// A regular file had more than one hard link.
    HardLinkedFile {
        /// The package-relative entry path.
        path: PathBuf,
    },
    /// A regular file had one or more executable mode bits.
    ExecutableFile {
        /// The package-relative entry path.
        path: PathBuf,
    },
    /// A configured limit was exceeded.
    LimitExceeded {
        /// The rejected limit.
        limit: PackageLimit,
        /// The configured maximum.
        maximum: u64,
        /// The observed value.
        actual: u64,
    },
    /// One named file exceeded the individual-file limit.
    FileTooLarge {
        /// The package-relative path or required file name.
        path: String,
        /// The configured maximum.
        maximum: u64,
        /// The observed byte length.
        actual: u64,
    },
    /// A Markdown operation referenced a non-Markdown source.
    MarkdownExtensionRequired(PayloadPath),
    /// An attachment destination used a disallowed extension.
    UnsupportedAttachment(String),
    /// An operation referenced a payload file that was absent.
    MissingPayload(PayloadPath),
    /// A payload file was not referenced by any operation.
    UnexpectedPayload(PayloadPath),
    /// A payload file changed while its digest was calculated.
    FileChangedDuringValidation(PayloadPath),
    /// A Markdown document's front matter was invalid or inconsistent.
    InvalidFrontMatter(markdown::MarkdownValidationError),
    /// The stored digest file was not canonical.
    InvalidStoredDigest,
    /// Gateway-owned acceptance ordering metadata was malformed.
    InvalidAcceptanceMetadata(serde_json::Error),
    /// Immutable accepted contents no longer matched the stored digest.
    StoredDigestMismatch {
        /// The digest recorded at acceptance.
        stored: PackageDigest,
        /// The digest calculated during revalidation.
        calculated: PackageDigest,
    },
}

impl PackageValidationError {
    /// Returns the stable protocol error code for this failure.
    #[must_use]
    pub const fn error_code(&self) -> ErrorCode {
        match self {
            Self::Io(_) => ErrorCode::TemporaryFailure,
            Self::InvalidPayloadPath(_)
            | Self::InvalidRequest(RequestDecodeError::InvalidPath { .. }) => {
                ErrorCode::InvalidPath
            }
            Self::InvalidRequestMetadata(RequestValidationError::UnsupportedProtocolVersion {
                ..
            }) => ErrorCode::InvalidProtocol,
            Self::LimitExceeded { .. } | Self::FileTooLarge { .. } => ErrorCode::LimitExceeded,
            Self::UnsupportedAttachment(_) => ErrorCode::UnsupportedFileType,
            Self::InvalidFrontMatter(error) => error.error_code(),
            Self::InvalidLayout
            | Self::MissingTopLevelEntry(_)
            | Self::UnexpectedTopLevelEntry(_)
            | Self::InvalidEntryType { .. }
            | Self::HardLinkedFile { .. }
            | Self::ExecutableFile { .. }
            | Self::EmptyPayloadDirectory(_)
            | Self::InvalidRequest(_)
            | Self::InvalidRequestMetadata(_)
            | Self::MarkdownExtensionRequired(_)
            | Self::MissingPayload(_)
            | Self::UnexpectedPayload(_)
            | Self::FileChangedDuringValidation(_) => ErrorCode::InvalidRequest,
            Self::CanonicalRequestJson(_) => ErrorCode::InternalError,
            Self::InvalidStoredDigest
            | Self::InvalidAcceptanceMetadata(_)
            | Self::StoredDigestMismatch { .. } => ErrorCode::ContentValidationFailed,
        }
    }
}

impl fmt::Display for PackageValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "package I/O failed: {error}"),
            Self::InvalidLayout => formatter.write_str("package layout is invalid"),
            Self::MissingTopLevelEntry(name) => {
                write!(formatter, "package is missing required `{name}`")
            }
            Self::UnexpectedTopLevelEntry(name) => {
                write!(
                    formatter,
                    "package contains unexpected top-level entry `{name}`"
                )
            }
            Self::InvalidEntryType { path } => {
                write!(
                    formatter,
                    "package entry `{}` has an invalid type",
                    path.display()
                )
            }
            Self::InvalidPayloadPath(path) => {
                write!(formatter, "payload path `{path}` is invalid")
            }
            Self::EmptyPayloadDirectory(path) => {
                write!(formatter, "payload directory `{path}` contains no files")
            }
            Self::InvalidRequest(error) => write!(formatter, "request is invalid: {error}"),
            Self::InvalidRequestMetadata(error) => {
                write!(formatter, "request metadata is invalid: {error}")
            }
            Self::CanonicalRequestJson(error) => {
                write!(formatter, "canonical request serialization failed: {error}")
            }
            Self::HardLinkedFile { path } => {
                write!(
                    formatter,
                    "package file `{}` must not be hard-linked",
                    path.display()
                )
            }
            Self::ExecutableFile { path } => {
                write!(
                    formatter,
                    "package file `{}` must not be executable",
                    path.display()
                )
            }
            Self::LimitExceeded {
                limit,
                maximum,
                actual,
            } => write!(
                formatter,
                "package {limit:?} is {actual}; configured maximum is {maximum}"
            ),
            Self::FileTooLarge {
                path,
                maximum,
                actual,
            } => write!(
                formatter,
                "package file `{path}` is {actual} bytes; configured maximum is {maximum}"
            ),
            Self::MarkdownExtensionRequired(path) => {
                write!(
                    formatter,
                    "Markdown source `{path}` must use the `.md` extension"
                )
            }
            Self::UnsupportedAttachment(name) => {
                write!(
                    formatter,
                    "attachment `{name}` has an unsupported extension"
                )
            }
            Self::MissingPayload(path) => {
                write!(formatter, "request references missing payload `{path}`")
            }
            Self::UnexpectedPayload(path) => {
                write!(
                    formatter,
                    "payload `{path}` is not referenced by the request"
                )
            }
            Self::FileChangedDuringValidation(path) => {
                write!(formatter, "payload `{path}` changed during validation")
            }
            Self::InvalidFrontMatter(error) => {
                write!(formatter, "Markdown front matter is invalid: {error}")
            }
            Self::InvalidStoredDigest => {
                formatter.write_str("stored package digest is not canonical")
            }
            Self::InvalidAcceptanceMetadata(error) => {
                write!(formatter, "acceptance metadata is invalid: {error}")
            }
            Self::StoredDigestMismatch { stored, calculated } => write!(
                formatter,
                "stored package digest `{stored}` does not match calculated digest `{calculated}`"
            ),
        }
    }
}

impl std::error::Error for PackageValidationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::InvalidRequest(error) => Some(error),
            Self::InvalidRequestMetadata(error) => Some(error),
            Self::CanonicalRequestJson(error) => Some(error),
            Self::InvalidAcceptanceMetadata(error) => Some(error),
            Self::InvalidFrontMatter(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use agent_knowledge_core::{ErrorCode, PayloadPath, RequestId};
use ulid::Ulid;

use crate::{
    PackageDigest, PackagePolicy, PackageValidationError, ValidatedPackage, validate_package,
};

const REQUEST_FILE_NAME: &str = "request.json";
const DIGEST_FILE_NAME: &str = "digest";
const PAYLOAD_DIRECTORY_NAME: &str = "payload";
const COPY_BUFFER_LENGTH: usize = 64 * 1024;
const MAXIMUM_DIGEST_FILE_BYTES: u64 = 72;
const MAXIMUM_STAGING_NAME_ATTEMPTS: usize = 16;

/// An accepted queue state represented by one directory below `queue/`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum QueueState {
    /// Accepted and waiting for the Repository Worker.
    Pending,
    /// Claimed by the Repository Worker.
    Processing,
    /// Successfully applied and published.
    Completed,
    /// Permanently rejected while applying.
    Failed,
}

impl QueueState {
    const ALL: [Self; 4] = [
        Self::Pending,
        Self::Processing,
        Self::Completed,
        Self::Failed,
    ];

    /// Returns the stable storage directory name.
    #[must_use]
    pub const fn directory_name(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Processing => "processing",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Display for QueueState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.directory_name())
    }
}

/// Result of atomically accepting one request package.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnqueueOutcome {
    /// A new immutable package was placed in `pending/`.
    Accepted {
        /// The accepted request identifier.
        request_id: RequestId,
        /// The normalized metadata and payload digest.
        digest: PackageDigest,
    },
    /// The same request identifier and digest already existed.
    Existing {
        /// The existing request identifier.
        request_id: RequestId,
        /// The matching normalized digest.
        digest: PackageDigest,
        /// The request's current accepted state.
        state: QueueState,
    },
}

/// A durable file-system queue rooted in the configured storage tree.
#[derive(Clone, Debug)]
pub struct FileQueue {
    queue_root: PathBuf,
    lock_file: PathBuf,
    policy: PackagePolicy,
}

impl FileQueue {
    /// Creates missing queue-state directories and opens a queue handle.
    ///
    /// The queue root and all state directories must reside on the same file
    /// system. The lock path may be placed in the storage tree's `locks/`
    /// directory.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured paths cannot be initialized as
    /// regular directories and a regular lock file.
    pub fn initialize(
        queue_root: impl Into<PathBuf>,
        lock_file: impl Into<PathBuf>,
        policy: PackagePolicy,
    ) -> Result<Self, QueueError> {
        let queue_root = queue_root.into();
        let lock_file = lock_file.into();

        ensure_directory(&queue_root)?;
        ensure_directory(&queue_root.join("incoming"))?;
        for state in QueueState::ALL {
            ensure_directory(&queue_root.join(state.directory_name()))?;
        }

        if let Some(parent) = lock_file.parent()
            && !parent.as_os_str().is_empty()
        {
            ensure_directory(parent)?;
        }
        ensure_lock_file(&lock_file)?;

        sync_directory(&queue_root)?;
        if let Some(parent) = queue_root.parent()
            && !parent.as_os_str().is_empty()
        {
            sync_directory(parent)?;
        }
        if let Some(parent) = lock_file.parent()
            && !parent.as_os_str().is_empty()
        {
            sync_directory(parent)?;
        }

        Ok(Self {
            queue_root,
            lock_file,
            policy,
        })
    }

    /// Creates an exclusive random package directory below `incoming/`.
    ///
    /// # Errors
    ///
    /// Returns an error when a staging directory cannot be created.
    pub fn begin(&self) -> Result<IncomingPackage, QueueError> {
        let incoming_root = self.queue_root.join("incoming");
        for _ in 0..MAXIMUM_STAGING_NAME_ATTEMPTS {
            let staging_name = format!(".incoming-{}", Ulid::generate());
            let staging_path = incoming_root.join(staging_name);
            match fs::create_dir(&staging_path) {
                Ok(()) => {
                    if let Err(error) = fs::create_dir(staging_path.join(PAYLOAD_DIRECTORY_NAME)) {
                        let _ = fs::remove_dir(&staging_path);
                        return Err(QueueError::Io(error));
                    }
                    return Ok(IncomingPackage {
                        queue: self.clone(),
                        staging_path,
                        written_payload: HashSet::new(),
                        written_files: 0,
                        written_bytes: 0,
                        request_written: false,
                        promoted: false,
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(QueueError::Io(error)),
            }
        }

        Err(QueueError::StagingNameExhausted)
    }

    fn state_path(&self, state: QueueState, request_id: RequestId) -> PathBuf {
        self.queue_root
            .join(state.directory_name())
            .join(request_id.to_string())
    }

    fn find_existing(
        &self,
        request_id: RequestId,
    ) -> Result<Option<(QueueState, PackageDigest)>, QueueError> {
        let mut existing = None;
        for state in QueueState::ALL {
            let path = self.state_path(state, request_id);
            match fs::symlink_metadata(&path) {
                Ok(metadata) => {
                    if !metadata.file_type().is_dir() {
                        return Err(QueueError::CorruptState {
                            request_id,
                            state,
                            detail: "request entry is not a directory",
                        });
                    }
                    if existing.is_some() {
                        return Err(QueueError::RequestInMultipleStates { request_id });
                    }
                    let digest = read_stored_digest(&path, request_id, state)?;
                    existing = Some((state, digest));
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(QueueError::Io(error)),
            }
        }
        Ok(existing)
    }
}

/// An exclusive, unaccepted request package below `queue/incoming/`.
pub struct IncomingPackage {
    queue: FileQueue,
    staging_path: PathBuf,
    written_payload: HashSet<PayloadPath>,
    written_files: usize,
    written_bytes: u64,
    request_written: bool,
    promoted: bool,
}

impl IncomingPackage {
    /// Streams `request.json` into the package without replacing an existing file.
    ///
    /// # Errors
    ///
    /// Returns an error for a duplicate request file, an I/O failure, or a
    /// configured byte or file-count limit.
    pub fn write_request(&mut self, source: impl Read) -> Result<(), QueueError> {
        if self.request_written {
            return Err(QueueError::RequestAlreadyWritten);
        }
        self.write_file(self.staging_path.join(REQUEST_FILE_NAME), source)?;
        self.request_written = true;
        Ok(())
    }

    /// Streams one payload file into the package using a normalized relative path.
    ///
    /// # Errors
    ///
    /// Returns an error for a duplicate payload path, an I/O failure, or a
    /// configured byte or file-count limit.
    pub fn add_payload(&mut self, path: PayloadPath, source: impl Read) -> Result<(), QueueError> {
        if self.written_payload.contains(&path) {
            return Err(QueueError::PayloadAlreadyWritten(path));
        }

        let destination = self
            .staging_path
            .join(PAYLOAD_DIRECTORY_NAME)
            .join(path.as_str());
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).map_err(QueueError::Io)?;
        }
        self.write_file(destination, source)?;
        self.written_payload.insert(path);
        Ok(())
    }

    /// Validates, synchronizes, and atomically moves this package to `pending/`.
    ///
    /// A retry with the same request ID and digest returns `Existing`. Reusing
    /// an ID for different contents is rejected without modifying the existing
    /// accepted request.
    ///
    /// # Errors
    ///
    /// Returns an error when validation, synchronization, idempotency checks,
    /// locking, or the atomic state transition fails.
    pub fn accept(self) -> Result<EnqueueOutcome, QueueError> {
        self.accept_with_hook(&mut NoopAcceptanceHook)
    }

    fn write_file(
        &mut self,
        destination: PathBuf,
        mut source: impl Read,
    ) -> Result<(), QueueError> {
        let limits = self.queue.policy.limits();
        let next_file_count =
            self.written_files
                .checked_add(1)
                .ok_or(QueueError::LimitExceeded {
                    limit: QueueLimit::FileCount,
                    maximum: limits.maximum_file_count as u64,
                    actual: u64::MAX,
                })?;
        if next_file_count > limits.maximum_file_count {
            return Err(QueueError::LimitExceeded {
                limit: QueueLimit::FileCount,
                maximum: limits.maximum_file_count as u64,
                actual: next_file_count as u64,
            });
        }

        let mut destination_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&destination)
            .map_err(QueueError::Io)?;
        let mut file_bytes = 0_u64;
        let mut buffer = [0_u8; COPY_BUFFER_LENGTH];

        let result = loop {
            let read = match source.read(&mut buffer) {
                Ok(read) => read,
                Err(error) => break Err(QueueError::Io(error)),
            };
            if read == 0 {
                break Ok(());
            }

            file_bytes = match file_bytes.checked_add(read as u64) {
                Some(bytes) => bytes,
                None => {
                    break Err(QueueError::LimitExceeded {
                        limit: QueueLimit::IndividualFileBytes,
                        maximum: limits.maximum_file_bytes,
                        actual: u64::MAX,
                    });
                }
            };
            if file_bytes > limits.maximum_file_bytes {
                break Err(QueueError::LimitExceeded {
                    limit: QueueLimit::IndividualFileBytes,
                    maximum: limits.maximum_file_bytes,
                    actual: file_bytes,
                });
            }

            let total_bytes = match self.written_bytes.checked_add(file_bytes) {
                Some(bytes) => bytes,
                None => {
                    break Err(QueueError::LimitExceeded {
                        limit: QueueLimit::TotalBytes,
                        maximum: limits.maximum_total_bytes,
                        actual: u64::MAX,
                    });
                }
            };
            if total_bytes > limits.maximum_total_bytes {
                break Err(QueueError::LimitExceeded {
                    limit: QueueLimit::TotalBytes,
                    maximum: limits.maximum_total_bytes,
                    actual: total_bytes,
                });
            }

            if let Err(error) = destination_file.write_all(&buffer[..read]) {
                break Err(QueueError::Io(error));
            }
        };

        drop(destination_file);
        if let Err(error) = result {
            let _ = fs::remove_file(destination);
            return Err(error);
        }

        self.written_files = next_file_count;
        self.written_bytes += file_bytes;
        Ok(())
    }

    fn accept_with_hook(
        mut self,
        hook: &mut dyn AcceptanceHook,
    ) -> Result<EnqueueOutcome, QueueError> {
        let validated = validate_package(&self.staging_path, &self.queue.policy)
            .map_err(QueueError::Package)?;
        write_digest_file(&self.staging_path, validated.digest())?;
        sync_package(&self.staging_path, &validated)?;
        hook.reached(AcceptancePhase::PackageSynchronized)
            .map_err(QueueError::Io)?;

        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.queue.lock_file)
            .map_err(QueueError::Io)?;
        lock.lock().map_err(QueueError::Io)?;

        let request_id = validated.request().request_id;
        let digest = validated.digest();
        if let Some((state, existing_digest)) = self.queue.find_existing(request_id)? {
            if existing_digest == digest {
                sync_directory(&self.queue.queue_root.join("incoming"))?;
                for accepted_state in QueueState::ALL {
                    sync_directory(&self.queue.queue_root.join(accepted_state.directory_name()))?;
                }
                hook.reached(AcceptancePhase::ExistingQueueDirectoriesSynchronized)
                    .map_err(QueueError::Io)?;
                return Ok(EnqueueOutcome::Existing {
                    request_id,
                    digest,
                    state,
                });
            }
            return Err(QueueError::RequestIdReused { request_id, state });
        }

        let pending_path = self.queue.state_path(QueueState::Pending, request_id);
        fs::rename(&self.staging_path, &pending_path).map_err(QueueError::Io)?;
        self.promoted = true;
        hook.reached(AcceptancePhase::Renamed)
            .map_err(QueueError::Io)?;

        sync_directory(&self.queue.queue_root.join("pending"))?;
        sync_directory(&self.queue.queue_root.join("incoming"))?;
        hook.reached(AcceptancePhase::QueueDirectoriesSynchronized)
            .map_err(QueueError::Io)?;

        Ok(EnqueueOutcome::Accepted { request_id, digest })
    }
}

impl Drop for IncomingPackage {
    fn drop(&mut self) {
        if !self.promoted {
            let _ = fs::remove_dir_all(&self.staging_path);
        }
    }
}

fn ensure_directory(path: &Path) -> Result<(), QueueError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(()),
        Ok(_) => Err(QueueError::InvalidStoragePath(path.into())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir_all(path).map_err(QueueError::Io)
        }
        Err(error) => Err(QueueError::Io(error)),
    }
}

fn ensure_lock_file(path: &Path) -> Result<(), QueueError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(()),
        Ok(_) => Err(QueueError::InvalidStoragePath(path.into())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            match OpenOptions::new().write(true).create_new(true).open(path) {
                Ok(_) => Ok(()),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    ensure_lock_file(path)
                }
                Err(error) => Err(QueueError::Io(error)),
            }
        }
        Err(error) => Err(QueueError::Io(error)),
    }
}

fn write_digest_file(package_root: &Path, digest: PackageDigest) -> Result<(), QueueError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(package_root.join(DIGEST_FILE_NAME))
        .map_err(QueueError::Io)?;
    writeln!(file, "{digest}").map_err(QueueError::Io)?;
    Ok(())
}

fn sync_package(package_root: &Path, package: &ValidatedPackage) -> Result<(), QueueError> {
    File::open(package_root.join(REQUEST_FILE_NAME))
        .map_err(QueueError::Io)?
        .sync_all()
        .map_err(QueueError::Io)?;
    File::open(package_root.join(DIGEST_FILE_NAME))
        .map_err(QueueError::Io)?
        .sync_all()
        .map_err(QueueError::Io)?;

    let payload_root = package_root.join(PAYLOAD_DIRECTORY_NAME);
    let mut directories = HashSet::new();
    directories.insert(payload_root.clone());
    for payload in package.payload() {
        File::open(payload_root.join(payload.path().as_str()))
            .map_err(QueueError::Io)?
            .sync_all()
            .map_err(QueueError::Io)?;

        let mut current = payload_root
            .join(payload.path().as_str())
            .parent()
            .map(Path::to_path_buf);
        while let Some(directory) = current {
            if !directory.starts_with(&payload_root) {
                break;
            }
            directories.insert(directory.clone());
            if directory == payload_root {
                break;
            }
            current = directory.parent().map(Path::to_path_buf);
        }
    }

    let mut directories = directories.into_iter().collect::<Vec<_>>();
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories {
        sync_directory(&directory)?;
    }
    sync_directory(package_root)?;
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), QueueError> {
    File::open(path)
        .map_err(QueueError::Io)?
        .sync_all()
        .map_err(QueueError::Io)
}

fn read_stored_digest(
    package_root: &Path,
    request_id: RequestId,
    state: QueueState,
) -> Result<PackageDigest, QueueError> {
    let digest_path = package_root.join(DIGEST_FILE_NAME);
    let digest_metadata = fs::symlink_metadata(&digest_path).map_err(QueueError::Io)?;
    if !digest_metadata.file_type().is_file() {
        return Err(QueueError::CorruptState {
            request_id,
            state,
            detail: "digest entry is not a regular file",
        });
    }
    let mut bytes = Vec::with_capacity(MAXIMUM_DIGEST_FILE_BYTES as usize);
    File::open(digest_path)
        .map_err(QueueError::Io)?
        .take(MAXIMUM_DIGEST_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(QueueError::Io)?;
    let contents = std::str::from_utf8(&bytes).map_err(|_| QueueError::CorruptState {
        request_id,
        state,
        detail: "digest is not UTF-8",
    })?;
    if bytes.len() as u64 > MAXIMUM_DIGEST_FILE_BYTES {
        return Err(QueueError::CorruptState {
            request_id,
            state,
            detail: "digest file is too large",
        });
    }
    let Some(value) = contents.strip_suffix('\n') else {
        return Err(QueueError::CorruptState {
            request_id,
            state,
            detail: "digest is not newline-terminated",
        });
    };
    if value.contains('\n') {
        return Err(QueueError::CorruptState {
            request_id,
            state,
            detail: "digest contains multiple lines",
        });
    }
    value.parse().map_err(|_| QueueError::CorruptState {
        request_id,
        state,
        detail: "digest is not a canonical SHA-256 value",
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AcceptancePhase {
    PackageSynchronized,
    Renamed,
    QueueDirectoriesSynchronized,
    ExistingQueueDirectoriesSynchronized,
}

trait AcceptanceHook {
    fn reached(&mut self, phase: AcceptancePhase) -> io::Result<()>;
}

struct NoopAcceptanceHook;

impl AcceptanceHook for NoopAcceptanceHook {
    fn reached(&mut self, _phase: AcceptancePhase) -> io::Result<()> {
        Ok(())
    }
}

/// A streaming queue limit that rejected package input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueueLimit {
    /// Combined request and payload bytes.
    TotalBytes,
    /// Bytes in one input file.
    IndividualFileBytes,
    /// Number of request and payload files.
    FileCount,
}

/// A durable queue operation failure.
#[derive(Debug)]
pub enum QueueError {
    /// A file-system or locking operation failed.
    Io(io::Error),
    /// Extracted package validation failed.
    Package(PackageValidationError),
    /// A configured storage path had the wrong file type.
    InvalidStoragePath(PathBuf),
    /// Exclusive random staging directory names were repeatedly occupied.
    StagingNameExhausted,
    /// `request.json` was supplied more than once.
    RequestAlreadyWritten,
    /// The same payload path was supplied more than once.
    PayloadAlreadyWritten(PayloadPath),
    /// A streaming input limit was exceeded.
    LimitExceeded {
        /// The rejected limit.
        limit: QueueLimit,
        /// The configured maximum.
        maximum: u64,
        /// The observed value.
        actual: u64,
    },
    /// An accepted request ID was reused with different normalized contents.
    RequestIdReused {
        /// The reused request identifier.
        request_id: RequestId,
        /// The existing request's current state.
        state: QueueState,
    },
    /// The same request ID appeared in more than one accepted state.
    RequestInMultipleStates {
        /// The duplicated request identifier.
        request_id: RequestId,
    },
    /// An existing accepted request package was internally inconsistent.
    CorruptState {
        /// The affected request identifier.
        request_id: RequestId,
        /// The affected queue state.
        state: QueueState,
        /// A non-sensitive static diagnostic.
        detail: &'static str,
    },
}

impl QueueError {
    /// Returns the stable protocol error code for this failure.
    #[must_use]
    pub const fn error_code(&self) -> ErrorCode {
        match self {
            Self::Io(_) | Self::StagingNameExhausted => ErrorCode::TemporaryFailure,
            Self::Package(error) => error.error_code(),
            Self::RequestAlreadyWritten | Self::PayloadAlreadyWritten(_) => {
                ErrorCode::InvalidRequest
            }
            Self::LimitExceeded { .. } => ErrorCode::LimitExceeded,
            Self::RequestIdReused { .. } => ErrorCode::RequestIdReused,
            Self::CorruptState { .. } => ErrorCode::ContentValidationFailed,
            Self::InvalidStoragePath(_) | Self::RequestInMultipleStates { .. } => {
                ErrorCode::InternalError
            }
        }
    }
}

impl fmt::Display for QueueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "queue I/O failed: {error}"),
            Self::Package(error) => write!(formatter, "package validation failed: {error}"),
            Self::InvalidStoragePath(path) => {
                write!(
                    formatter,
                    "queue storage path `{}` has an invalid type",
                    path.display()
                )
            }
            Self::StagingNameExhausted => {
                formatter.write_str("could not allocate a unique incoming package directory")
            }
            Self::RequestAlreadyWritten => {
                formatter.write_str("request.json was already written to this package")
            }
            Self::PayloadAlreadyWritten(path) => {
                write!(
                    formatter,
                    "payload `{path}` was already written to this package"
                )
            }
            Self::LimitExceeded {
                limit,
                maximum,
                actual,
            } => write!(
                formatter,
                "queue input {limit:?} is {actual}; configured maximum is {maximum}"
            ),
            Self::RequestIdReused { request_id, state } => write!(
                formatter,
                "request ID `{request_id}` already exists in `{state}` with different contents"
            ),
            Self::RequestInMultipleStates { request_id } => {
                write!(
                    formatter,
                    "request ID `{request_id}` appears in multiple queue states"
                )
            }
            Self::CorruptState {
                request_id,
                state,
                detail,
            } => write!(
                formatter,
                "request `{request_id}` in `{state}` has corrupt queue state: {detail}"
            ),
        }
    }
}

impl std::error::Error for QueueError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Package(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests;

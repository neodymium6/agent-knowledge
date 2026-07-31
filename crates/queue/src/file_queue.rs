use std::collections::HashSet;
use std::fmt;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Read, Write};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use agent_knowledge_core::{ErrorCode, PayloadPath, RequestId, Revision};
use sha2::{Digest, Sha256};
use ulid::Ulid;

use crate::{
    AcceptanceMetadata, PackageDigest, PackagePolicy, PackageValidationError, ValidatedPackage,
    validate_accepted_package, validate_package,
};

mod worker;
pub use worker::{
    BatchClaimOutcome, CURRENT_WORKER_PHASE_SCHEMA_VERSION, CURRENT_WORKER_RESULT_SCHEMA_VERSION,
    ClaimToken, ClaimedPackage, ProcessingScanOutcome, WorkerPhase, WorkerPhaseRecord,
    WorkerQueueError, WorkerResultRecord, WorkerResultStatus, WorkerSession,
};

const REQUEST_FILE_NAME: &str = "request.json";
const DIGEST_FILE_NAME: &str = "digest";
const ACCEPTANCE_FILE_NAME: &str = "acceptance.json";
const NEXT_SEQUENCE_FILE_NAME: &str = "next-sequence";
const QUEUE_IDENTITY_FILE_NAME: &str = "queue-id";
const QUEUE_ROOT_BINDING_FILE_NAME: &str = "queue-root-binding-v1";
const QUARANTINE_MARKER_FILE_NAME: &str = ".quarantined-at";
const WORKER_TEMP_DIRECTORY_NAME: &str = "worker-tmp";
const LOCK_DIRECTORY_NAME: &str = ".locks";
const QUEUE_LOCK_FILE_NAME: &str = "queue.lock";
const WORKER_LOCK_FILE_NAME: &str = "repository-writer.lock";
const PAYLOAD_DIRECTORY_NAME: &str = "payload";
const COPY_BUFFER_LENGTH: usize = 64 * 1024;
const MAXIMUM_DIGEST_FILE_BYTES: u64 = 72;
const MAXIMUM_SEQUENCE_FILE_BYTES: u64 = 32;
const MAXIMUM_QUEUE_IDENTITY_BYTES: u64 = 72;
const MAXIMUM_QUARANTINE_MARKER_BYTES: u64 = 64;
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

    const fn index(self) -> usize {
        match self {
            Self::Pending => 0,
            Self::Processing => 1,
            Self::Completed => 2,
            Self::Failed => 3,
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
    configured_queue_root: PathBuf,
    root_handle: Arc<File>,
    directories: Arc<QueueDirectories>,
    identity: Revision,
    lock_file: PathBuf,
    stable_lock_file: PathBuf,
    queue_lock_handle: Arc<File>,
    worker_lock_file: PathBuf,
    stable_worker_lock_file: PathBuf,
    worker_lock_handle: Arc<File>,
    policy: PackagePolicy,
    maintenance_scanners: Arc<Mutex<MaintenanceScanners>>,
}

#[derive(Debug)]
struct QueueDirectories {
    lock: PinnedDirectory,
    incoming: PinnedDirectory,
    quarantine: PinnedDirectory,
    worker_temporary: PinnedDirectory,
    states: [PinnedDirectory; 4],
}

impl QueueDirectories {
    fn state(&self, state: QueueState) -> &PinnedDirectory {
        &self.states[state.index()]
    }

    fn all(&self) -> impl Iterator<Item = &PinnedDirectory> {
        [
            &self.lock,
            &self.incoming,
            &self.quarantine,
            &self.worker_temporary,
        ]
        .into_iter()
        .chain(self.states.iter())
    }
}

#[derive(Debug)]
struct PinnedDirectory {
    entry: PathBuf,
    stable: PathBuf,
    handle: Arc<File>,
}

impl FileQueue {
    /// Creates missing queue-state directories and opens a queue handle.
    ///
    /// The queue root and all state directories must reside on the same file
    /// system. Queue and Repository Worker locks use fixed names below the queue
    /// root's `.locks/` directory, so independently initialized handles for the
    /// same queue cannot select different lock identities.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured queue root cannot be initialized
    /// with regular state and lock directories and regular lock files.
    pub fn initialize(
        queue_root: impl Into<PathBuf>,
        policy: PackagePolicy,
    ) -> Result<Self, QueueError> {
        let configured_path = queue_root.into();
        let configured_parent = configured_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let parent_metadata = fs::metadata(configured_parent).map_err(QueueError::Io)?;
        if !parent_metadata.is_dir() {
            return Err(QueueError::InvalidStoragePath(configured_parent.into()));
        }
        ensure_directory(&configured_path)?;
        sync_directory(configured_parent)?;
        let configured_queue_root = fs::canonicalize(&configured_path).map_err(QueueError::Io)?;
        let root_handle = Arc::new(File::open(&configured_queue_root).map_err(QueueError::Io)?);
        #[cfg(target_os = "linux")]
        let queue_root = {
            use std::os::fd::AsRawFd;
            PathBuf::from(format!("/proc/self/fd/{}", root_handle.as_raw_fd()))
        };
        #[cfg(not(target_os = "linux"))]
        let queue_root = configured_queue_root.clone();
        fs::metadata(&queue_root).map_err(QueueError::Io)?;

        ensure_directory(&queue_root.join(LOCK_DIRECTORY_NAME))?;
        ensure_directory(&queue_root.join("incoming"))?;
        ensure_directory(&queue_root.join("quarantine"))?;
        ensure_directory(&queue_root.join(WORKER_TEMP_DIRECTORY_NAME))?;
        for state in QueueState::ALL {
            ensure_directory(&queue_root.join(state.directory_name()))?;
        }
        sync_directory(&queue_root)?;
        let directories = Arc::new(QueueDirectories {
            lock: pin_directory(&queue_root.join(LOCK_DIRECTORY_NAME))?,
            incoming: pin_directory(&queue_root.join("incoming"))?,
            quarantine: pin_directory(&queue_root.join("quarantine"))?,
            worker_temporary: pin_directory(&queue_root.join(WORKER_TEMP_DIRECTORY_NAME))?,
            states: [
                pin_directory(&queue_root.join(QueueState::Pending.directory_name()))?,
                pin_directory(&queue_root.join(QueueState::Processing.directory_name()))?,
                pin_directory(&queue_root.join(QueueState::Completed.directory_name()))?,
                pin_directory(&queue_root.join(QueueState::Failed.directory_name()))?,
            ],
        });
        validate_common_queue_mount(&root_handle, &directories)?;
        let lock_file = directories.lock.stable.join(QUEUE_LOCK_FILE_NAME);
        let worker_lock_file = directories.lock.stable.join(WORKER_LOCK_FILE_NAME);

        ensure_lock_file(&lock_file)?;
        ensure_lock_file(&worker_lock_file)?;
        let queue_lock_handle = Arc::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&lock_file)
                .map_err(QueueError::Io)?,
        );
        let worker_lock_handle = Arc::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&worker_lock_file)
                .map_err(QueueError::Io)?,
        );
        let stable_lock_file = stable_file_path(&queue_lock_handle, &lock_file)?;
        let stable_worker_lock_file = stable_file_path(&worker_lock_handle, &worker_lock_file)?;
        queue_lock_handle.sync_all().map_err(QueueError::Io)?;
        worker_lock_handle.sync_all().map_err(QueueError::Io)?;
        sync_directory(&directories.lock.stable)?;
        let initialization_lock = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&stable_lock_file)
            .map_err(QueueError::Io)?;
        initialization_lock.lock().map_err(QueueError::Io)?;
        ensure_queue_root_binding(
            &queue_root.join(QUEUE_ROOT_BINDING_FILE_NAME),
            &configured_queue_root,
            &root_handle,
            &directories,
            &queue_lock_handle,
            &worker_lock_handle,
            &queue_root,
        )?;
        let identity =
            ensure_queue_identity_file(&queue_root.join(QUEUE_IDENTITY_FILE_NAME), &queue_root)?;
        ensure_sequence_file(&queue_root.join(NEXT_SEQUENCE_FILE_NAME), &queue_root)?;

        sync_directory(&queue_root)?;
        if let Some(parent) = configured_queue_root.parent()
            && !parent.as_os_str().is_empty()
        {
            sync_directory(parent)?;
        }
        let queue = Self {
            configured_queue_root,
            directories,
            identity,
            lock_file,
            stable_lock_file,
            queue_lock_handle,
            worker_lock_file,
            stable_worker_lock_file,
            worker_lock_handle,
            queue_root,
            root_handle,
            policy,
            maintenance_scanners: Arc::new(Mutex::new(MaintenanceScanners::default())),
        };
        queue.current_identity_locked()?;
        Ok(queue)
    }

    /// Creates an exclusive random package directory below `incoming/`.
    ///
    /// # Errors
    ///
    /// Returns an error when a staging directory cannot be created.
    pub fn begin(&self) -> Result<IncomingPackage, QueueError> {
        let queue_lock = self.open_queue_lock()?;
        queue_lock.lock().map_err(QueueError::Io)?;
        self.current_identity_locked()?;
        let incoming_root = &self.directories.incoming.stable;
        for _ in 0..MAXIMUM_STAGING_NAME_ATTEMPTS {
            let staging_name = format!(".incoming-{}", Ulid::generate());
            let staging_path = incoming_root.join(staging_name);
            match fs::create_dir(&staging_path) {
                Ok(()) => {
                    if let Err(error) = fs::create_dir(staging_path.join(PAYLOAD_DIRECTORY_NAME)) {
                        let _ = fs::remove_dir(&staging_path);
                        return Err(QueueError::Io(error));
                    }
                    let lease = match File::open(&staging_path) {
                        Ok(lease) => lease,
                        Err(error) => {
                            let _ = fs::remove_dir_all(&staging_path);
                            return Err(QueueError::Io(error));
                        }
                    };
                    if let Err(error) = lease.lock() {
                        let _ = fs::remove_dir_all(&staging_path);
                        return Err(QueueError::Io(error));
                    }
                    if let Err(error) = self.current_identity_locked() {
                        drop(lease);
                        let _ = fs::remove_dir_all(&staging_path);
                        return Err(error);
                    }
                    drop(queue_lock);
                    return Ok(IncomingPackage {
                        queue: self.clone(),
                        staging_path,
                        _lease: lease,
                        written_payload: HashSet::new(),
                        written_directories: HashSet::new(),
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
        self.state_root(state).join(request_id.to_string())
    }

    fn state_root(&self, state: QueueState) -> &Path {
        &self.directories.state(state).stable
    }

    fn worker_temporary_root(&self) -> &Path {
        &self.directories.worker_temporary.stable
    }

    /// Moves inactive stale staging directories into `quarantine/`.
    ///
    /// Active packages are protected by a directory lease and are never moved.
    /// Quarantined data is retained for a separate administrative reap.
    ///
    /// # Errors
    ///
    /// Returns an error when queue locking, age inspection, lease acquisition,
    /// rename, or directory synchronization fails.
    pub fn quarantine_stale_incoming(
        &self,
        minimum_age: Duration,
        maximum_scan_entries: usize,
        maximum_actions: usize,
    ) -> Result<usize, QueueError> {
        let lock = self.open_queue_lock()?;
        lock.lock().map_err(QueueError::Io)?;
        self.current_identity_locked()?;

        let incoming_root = &self.directories.incoming.stable;
        let quarantine_root = &self.directories.quarantine.stable;
        let candidates = {
            let mut scanners = self
                .maintenance_scanners
                .lock()
                .map_err(|_| QueueError::MaintenanceScannerPoisoned)?;
            inactive_stale_directories(
                incoming_root,
                minimum_age,
                maximum_scan_entries,
                maximum_actions,
                StaleAgeSource::Directory,
                &mut scanners.incoming,
            )?
        };
        let mut moved = 0_usize;
        for candidate in candidates {
            let name = candidate
                .file_name()
                .ok_or_else(|| QueueError::InvalidStoragePath(candidate.clone()))?;
            remove_incoming_quarantine_marker(&candidate)?;
            let destination = quarantine_root.join(name);
            fs::rename(&candidate, &destination).map_err(QueueError::Io)?;
            if let Err(error) = replace_quarantine_marker(&destination) {
                sync_directory(quarantine_root)?;
                sync_directory(incoming_root)?;
                return Err(error);
            }
            moved += 1;
        }
        if moved > 0 {
            sync_directory(quarantine_root)?;
            sync_directory(incoming_root)?;
        }
        self.current_identity_locked()?;
        Ok(moved)
    }

    /// Permanently removes inactive stale directories already in `quarantine/`.
    ///
    /// This operation never scans or removes accepted queue states.
    ///
    /// # Errors
    ///
    /// Returns an error when queue locking, age inspection, lease acquisition,
    /// removal, or directory synchronization fails.
    pub fn reap_quarantined_incoming(
        &self,
        minimum_age: Duration,
        maximum_scan_entries: usize,
        maximum_actions: usize,
    ) -> Result<usize, QueueError> {
        let lock = self.open_queue_lock()?;
        lock.lock().map_err(QueueError::Io)?;
        self.current_identity_locked()?;

        let quarantine_root = &self.directories.quarantine.stable;
        let candidates = {
            let mut scanners = self
                .maintenance_scanners
                .lock()
                .map_err(|_| QueueError::MaintenanceScannerPoisoned)?;
            inactive_stale_directories(
                quarantine_root,
                minimum_age,
                maximum_scan_entries,
                maximum_actions,
                StaleAgeSource::QuarantineMarker,
                &mut scanners.quarantine,
            )?
        };
        let removed = candidates.len();
        for candidate in candidates {
            fs::remove_dir_all(candidate).map_err(QueueError::Io)?;
        }
        if removed > 0 {
            sync_directory(quarantine_root)?;
        }
        self.current_identity_locked()?;
        Ok(removed)
    }

    fn open_queue_lock(&self) -> Result<File, QueueError> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.stable_lock_file)
            .map_err(QueueError::Io)
    }

    fn current_identity_locked(&self) -> Result<Revision, QueueError> {
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::MetadataExt;

            let pinned = self.root_handle.metadata().map_err(QueueError::Io)?;
            let configured =
                fs::symlink_metadata(&self.configured_queue_root).map_err(QueueError::Io)?;
            if !configured.file_type().is_dir()
                || pinned.dev() != configured.dev()
                || pinned.ino() != configured.ino()
            {
                return Err(QueueError::InvalidQueueIdentity);
            }
        }
        for directory in self.directories.all() {
            validate_pinned_directory(directory)?;
        }
        validate_pinned_lock(&self.lock_file, &self.queue_lock_handle)?;
        validate_pinned_lock(&self.worker_lock_file, &self.worker_lock_handle)?;
        validate_queue_root_binding(
            &self.queue_root.join(QUEUE_ROOT_BINDING_FILE_NAME),
            &self.configured_queue_root,
            &self.root_handle,
            &self.directories,
            &self.queue_lock_handle,
            &self.worker_lock_handle,
        )?;
        let stable_identity = read_queue_identity(&self.queue_root.join(QUEUE_IDENTITY_FILE_NAME))?;
        let configured_identity =
            read_queue_identity(&self.configured_queue_root.join(QUEUE_IDENTITY_FILE_NAME))?;
        if stable_identity == self.identity && configured_identity == self.identity {
            Ok(stable_identity)
        } else {
            Err(QueueError::InvalidQueueIdentity)
        }
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
    _lease: File,
    written_payload: HashSet<PayloadPath>,
    written_directories: HashSet<String>,
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

        let limits = self.queue.policy.limits();
        let components = path.as_str().split('/').collect::<Vec<_>>();
        if components.len() > limits.maximum_path_components {
            return Err(QueueError::LimitExceeded {
                limit: QueueLimit::PathComponents,
                maximum: limits.maximum_path_components as u64,
                actual: components.len() as u64,
            });
        }

        let mut parents = Vec::with_capacity(components.len().saturating_sub(1));
        let mut prefix = String::new();
        for component in &components[..components.len().saturating_sub(1)] {
            if !prefix.is_empty() {
                prefix.push('/');
            }
            prefix.push_str(component);
            if self
                .written_payload
                .iter()
                .any(|written| written.as_str() == prefix)
            {
                return Err(QueueError::PayloadPrefixCollision(path));
            }
            parents.push(prefix.clone());
        }
        if self.written_directories.contains(path.as_str()) {
            return Err(QueueError::PayloadPrefixCollision(path));
        }

        let new_directories = parents
            .iter()
            .filter(|parent| !self.written_directories.contains(parent.as_str()))
            .count();
        let directory_count = self
            .written_directories
            .len()
            .checked_add(new_directories)
            .ok_or(QueueError::LimitExceeded {
                limit: QueueLimit::DirectoryCount,
                maximum: limits.maximum_directory_count as u64,
                actual: u64::MAX,
            })?;
        if directory_count > limits.maximum_directory_count {
            return Err(QueueError::LimitExceeded {
                limit: QueueLimit::DirectoryCount,
                maximum: limits.maximum_directory_count as u64,
                actual: directory_count as u64,
            });
        }
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
        let entry_count =
            directory_count
                .checked_add(next_file_count)
                .ok_or(QueueError::LimitExceeded {
                    limit: QueueLimit::EntryCount,
                    maximum: limits.maximum_entry_count as u64,
                    actual: u64::MAX,
                })?;
        if entry_count > limits.maximum_entry_count {
            return Err(QueueError::LimitExceeded {
                limit: QueueLimit::EntryCount,
                maximum: limits.maximum_entry_count as u64,
                actual: entry_count as u64,
            });
        }

        let destination = self
            .staging_path
            .join(PAYLOAD_DIRECTORY_NAME)
            .join(path.as_str());
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).map_err(QueueError::Io)?;
        }
        self.written_directories.extend(parents);
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
        let next_entry_count = self
            .written_directories
            .len()
            .checked_add(next_file_count)
            .ok_or(QueueError::LimitExceeded {
                limit: QueueLimit::EntryCount,
                maximum: limits.maximum_entry_count as u64,
                actual: u64::MAX,
            })?;
        if next_entry_count > limits.maximum_entry_count {
            return Err(QueueError::LimitExceeded {
                limit: QueueLimit::EntryCount,
                maximum: limits.maximum_entry_count as u64,
                actual: next_entry_count as u64,
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

        let lock = self.queue.open_queue_lock()?;
        lock.lock().map_err(QueueError::Io)?;
        self.queue.current_identity_locked()?;

        let request_id = validated.request().request_id;
        let digest = validated.digest();
        if let Some((state, _stored_digest)) = self.queue.find_existing(request_id)? {
            let existing_path = self.queue.state_path(state, request_id);
            let existing =
                validate_accepted_package(&existing_path, &self.queue.policy).map_err(|error| {
                    match error {
                        PackageValidationError::Io(error) => QueueError::Io(error),
                        _ => QueueError::CorruptState {
                            request_id,
                            state,
                            detail: "accepted package failed immutable revalidation",
                        },
                    }
                })?;
            if existing.request().request_id != request_id {
                return Err(QueueError::CorruptState {
                    request_id,
                    state,
                    detail: "accepted package identity does not match its queue entry",
                });
            }
            if existing.digest() == digest {
                sync_directory(&self.queue.directories.incoming.stable)?;
                for accepted_state in QueueState::ALL {
                    sync_directory(self.queue.state_root(accepted_state))?;
                }
                hook.reached(AcceptancePhase::ExistingQueueDirectoriesSynchronized)
                    .map_err(QueueError::Io)?;
                self.queue.current_identity_locked()?;
                return Ok(EnqueueOutcome::Existing {
                    request_id,
                    digest,
                    state,
                });
            }
            return Err(QueueError::RequestIdReused { request_id, state });
        }

        let sequence = allocate_sequence(&self.queue.queue_root)?;
        let acceptance = AcceptanceMetadata {
            sequence: NonZeroU64::new(sequence).ok_or(QueueError::InvalidSequenceState)?,
            accepted_at: time::OffsetDateTime::now_utc(),
        };
        write_acceptance_file(&self.staging_path, acceptance)?;
        sync_file(&self.staging_path.join(ACCEPTANCE_FILE_NAME))?;
        sync_directory(&self.staging_path)?;
        hook.reached(AcceptancePhase::AcceptanceMetadataSynchronized)
            .map_err(QueueError::Io)?;

        let pending_path = self.queue.state_path(QueueState::Pending, request_id);
        self.queue.current_identity_locked()?;
        fs::rename(&self.staging_path, &pending_path).map_err(QueueError::Io)?;
        self.promoted = true;
        hook.reached(AcceptancePhase::Renamed)
            .map_err(QueueError::Io)?;

        sync_directory(self.queue.state_root(QueueState::Pending))?;
        sync_directory(&self.queue.directories.incoming.stable)?;
        hook.reached(AcceptancePhase::QueueDirectoriesSynchronized)
            .map_err(QueueError::Io)?;
        self.queue.current_identity_locked()?;

        Ok(EnqueueOutcome::Accepted { request_id, digest })
    }
}

#[derive(Debug, Default)]
struct MaintenanceScanners {
    incoming: DirectoryScanner,
    quarantine: DirectoryScanner,
}

#[derive(Debug, Default)]
struct DirectoryScanner {
    entries: Option<fs::ReadDir>,
}

fn inactive_stale_directories(
    root: &Path,
    minimum_age: Duration,
    maximum_scan_entries: usize,
    maximum_actions: usize,
    age_source: StaleAgeSource,
    scanner: &mut DirectoryScanner,
) -> Result<Vec<PathBuf>, QueueError> {
    if maximum_scan_entries == 0 || maximum_actions == 0 {
        return Ok(Vec::new());
    }
    if scanner.entries.is_none() {
        scanner.entries = Some(fs::read_dir(root).map_err(QueueError::Io)?);
    }
    let now = SystemTime::now();
    let mut stale = Vec::with_capacity(maximum_actions.min(maximum_scan_entries));
    let mut scanned = 0_usize;
    let mut actions = 0_usize;
    while scanned < maximum_scan_entries && actions < maximum_actions {
        let Some(entries) = scanner.entries.as_mut() else {
            break;
        };
        let Some(entry) = entries.next() else {
            scanner.entries = None;
            break;
        };
        scanned += 1;
        let entry = entry.map_err(QueueError::Io)?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !is_staging_directory_name(name) {
            continue;
        }
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path).map_err(QueueError::Io)?;
        if !metadata.file_type().is_dir() {
            continue;
        }
        let modified = match age_source {
            StaleAgeSource::Directory => metadata.modified().map_err(QueueError::Io)?,
            StaleAgeSource::QuarantineMarker => match inspect_quarantine_marker(&path)? {
                QuarantineMarker::Complete(modified) => modified,
                QuarantineMarker::MissingOrIncomplete => {
                    replace_quarantine_marker(&path)?;
                    actions += 1;
                    continue;
                }
                QuarantineMarker::UnsafeType => continue,
            },
        };
        let Ok(age) = now.duration_since(modified) else {
            continue;
        };
        if age < minimum_age {
            continue;
        }

        let lease = File::open(&path).map_err(QueueError::Io)?;
        match lease.try_lock() {
            Ok(()) => {
                stale.push(path);
                actions += 1;
            }
            Err(TryLockError::WouldBlock) => {}
            Err(TryLockError::Error(error)) => return Err(QueueError::Io(error)),
        }
    }
    Ok(stale)
}

#[derive(Clone, Copy)]
enum StaleAgeSource {
    Directory,
    QuarantineMarker,
}

enum QuarantineMarker {
    Complete(SystemTime),
    MissingOrIncomplete,
    UnsafeType,
}

fn inspect_quarantine_marker(package_root: &Path) -> Result<QuarantineMarker, QueueError> {
    let marker_path = package_root.join(QUARANTINE_MARKER_FILE_NAME);
    let metadata = match fs::symlink_metadata(&marker_path) {
        Ok(metadata) if metadata.file_type().is_file() => metadata,
        Ok(_) => return Ok(QuarantineMarker::UnsafeType),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(QuarantineMarker::MissingOrIncomplete);
        }
        Err(error) => return Err(QueueError::Io(error)),
    };
    let mut bytes = Vec::with_capacity(MAXIMUM_QUARANTINE_MARKER_BYTES as usize);
    File::open(&marker_path)
        .map_err(QueueError::Io)?
        .take(MAXIMUM_QUARANTINE_MARKER_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(QueueError::Io)?;
    if bytes.len() as u64 > MAXIMUM_QUARANTINE_MARKER_BYTES {
        return Ok(QuarantineMarker::MissingOrIncomplete);
    }
    let Ok(contents) = std::str::from_utf8(&bytes) else {
        return Ok(QuarantineMarker::MissingOrIncomplete);
    };
    let Some(timestamp) = contents.strip_suffix('\n') else {
        return Ok(QuarantineMarker::MissingOrIncomplete);
    };
    if timestamp.contains('\n')
        || time::OffsetDateTime::parse(timestamp, &time::format_description::well_known::Rfc3339)
            .is_err()
    {
        return Ok(QuarantineMarker::MissingOrIncomplete);
    }
    Ok(QuarantineMarker::Complete(
        metadata.modified().map_err(QueueError::Io)?,
    ))
}

fn remove_incoming_quarantine_marker(package_root: &Path) -> Result<(), QueueError> {
    let marker_path = package_root.join(QUARANTINE_MARKER_FILE_NAME);
    match fs::symlink_metadata(&marker_path) {
        Ok(metadata) if metadata.file_type().is_file() => {
            fs::remove_file(marker_path).map_err(QueueError::Io)?;
            sync_directory(package_root)
        }
        Ok(_) => Err(QueueError::InvalidStoragePath(marker_path)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(QueueError::Io(error)),
    }
}

fn replace_quarantine_marker(package_root: &Path) -> Result<(), QueueError> {
    let marker_path = package_root.join(QUARANTINE_MARKER_FILE_NAME);
    match fs::symlink_metadata(&marker_path) {
        Ok(metadata) if metadata.file_type().is_file() => {}
        Ok(_) => return Err(QueueError::InvalidStoragePath(marker_path)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(QueueError::Io(error)),
    }
    let temporary_path = package_root.join(format!(".quarantine-marker-{}", Ulid::generate()));
    let result = (|| {
        let mut marker = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary_path)
            .map_err(QueueError::Io)?;
        let timestamp = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(QueueError::QuarantineTimestamp)?;
        writeln!(marker, "{timestamp}").map_err(QueueError::Io)?;
        marker.sync_all().map_err(QueueError::Io)?;
        drop(marker);
        fs::rename(&temporary_path, marker_path).map_err(QueueError::Io)?;
        sync_directory(package_root)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary_path);
    }
    result
}

fn is_staging_directory_name(name: &str) -> bool {
    let Some(value) = name.strip_prefix(".incoming-") else {
        return false;
    };
    value
        .parse::<Ulid>()
        .is_ok_and(|identifier| identifier.to_string() == value)
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
        Err(error) if error.kind() == io::ErrorKind::NotFound => match fs::create_dir(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                match fs::symlink_metadata(path) {
                    Ok(metadata) if metadata.file_type().is_dir() => Ok(()),
                    Ok(_) => Err(QueueError::InvalidStoragePath(path.into())),
                    Err(error) => Err(QueueError::Io(error)),
                }
            }
            Err(error) => Err(QueueError::Io(error)),
        },
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

fn ensure_sequence_file(path: &Path, queue_root: &Path) -> Result<(), QueueError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => {
            let _ = read_next_sequence(path)?;
            Ok(())
        }
        Ok(_) => Err(QueueError::InvalidStoragePath(path.into())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if !accepted_states_are_empty(queue_root)? {
                return Err(QueueError::InvalidSequenceState);
            }
            match OpenOptions::new().write(true).create_new(true).open(path) {
                Ok(mut file) => {
                    file.write_all(b"1\n").map_err(QueueError::Io)?;
                    file.sync_all().map_err(QueueError::Io)
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    ensure_sequence_file(path, queue_root)
                }
                Err(error) => Err(QueueError::Io(error)),
            }
        }
        Err(error) => Err(QueueError::Io(error)),
    }
}

fn ensure_queue_identity_file(path: &Path, queue_root: &Path) -> Result<Revision, QueueError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => read_queue_identity(path),
        Ok(_) => Err(QueueError::InvalidStoragePath(path.into())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if !accepted_states_are_empty(queue_root)? {
                return Err(QueueError::InvalidQueueIdentity);
            }
            let mut hasher = Sha256::new();
            hasher.update(b"agent-knowledge-queue-instance-v1\0");
            hasher.update(Ulid::generate().to_bytes());
            let identity = Revision::from_bytes(hasher.finalize().into());
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
                .map_err(QueueError::Io)?;
            writeln!(file, "{identity}").map_err(QueueError::Io)?;
            file.sync_all().map_err(QueueError::Io)?;
            sync_directory(queue_root)?;
            Ok(identity)
        }
        Err(error) => Err(QueueError::Io(error)),
    }
}

fn ensure_queue_root_binding(
    path: &Path,
    configured_root: &Path,
    root_handle: &File,
    directories: &QueueDirectories,
    queue_lock_handle: &File,
    worker_lock_handle: &File,
    queue_root: &Path,
) -> Result<(), QueueError> {
    let expected = queue_root_binding(
        configured_root,
        root_handle,
        directories,
        queue_lock_handle,
        worker_lock_handle,
    )?;
    match fs::symlink_metadata(path) {
        Ok(_) => validate_queue_root_binding(
            path,
            configured_root,
            root_handle,
            directories,
            queue_lock_handle,
            worker_lock_handle,
        ),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if !accepted_states_are_empty(queue_root)? {
                return Err(QueueError::InvalidQueueIdentity);
            }
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
                .map_err(QueueError::Io)?;
            file.write_all(&expected).map_err(QueueError::Io)?;
            file.sync_all().map_err(QueueError::Io)?;
            sync_directory(queue_root)
        }
        Err(error) => Err(QueueError::Io(error)),
    }
}

fn queue_root_binding(
    configured_root: &Path,
    root_handle: &File,
    directories: &QueueDirectories,
    queue_lock_handle: &File,
    worker_lock_handle: &File,
) -> Result<Vec<u8>, QueueError> {
    let mut expected = configured_root.as_os_str().as_encoded_bytes().to_vec();
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let handles = std::iter::once(root_handle)
            .chain(directories.all().map(|directory| directory.handle.as_ref()))
            .chain([queue_lock_handle, worker_lock_handle]);
        for handle in handles {
            let metadata = handle.metadata().map_err(QueueError::Io)?;
            expected.push(0);
            expected.extend_from_slice(&metadata.dev().to_le_bytes());
            expected.extend_from_slice(&metadata.ino().to_le_bytes());
        }
    }
    Ok(expected)
}

fn validate_queue_root_binding(
    path: &Path,
    configured_root: &Path,
    root_handle: &File,
    directories: &QueueDirectories,
    queue_lock_handle: &File,
    worker_lock_handle: &File,
) -> Result<(), QueueError> {
    let expected = queue_root_binding(
        configured_root,
        root_handle,
        directories,
        queue_lock_handle,
        worker_lock_handle,
    )?;
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() && metadata.len() <= 16 * 1024 => {
            if fs::read(path).map_err(QueueError::Io)? == expected {
                Ok(())
            } else {
                Err(QueueError::InvalidQueueIdentity)
            }
        }
        Ok(_) => Err(QueueError::InvalidQueueIdentity),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            Err(QueueError::InvalidQueueIdentity)
        }
        Err(error) => Err(QueueError::Io(error)),
    }
}

fn validate_pinned_lock(path: &Path, pinned: &File) -> Result<(), QueueError> {
    let configured = fs::symlink_metadata(path).map_err(QueueError::Io)?;
    let pinned = pinned.metadata().map_err(QueueError::Io)?;
    if !configured.file_type().is_file() {
        return Err(QueueError::InvalidQueueIdentity);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if configured.dev() != pinned.dev() || configured.ino() != pinned.ino() {
            return Err(QueueError::InvalidQueueIdentity);
        }
    }
    Ok(())
}

fn pin_directory(path: &Path) -> Result<PinnedDirectory, QueueError> {
    let metadata = fs::symlink_metadata(path).map_err(QueueError::Io)?;
    if !metadata.file_type().is_dir() {
        return Err(QueueError::InvalidStoragePath(path.into()));
    }
    let handle = Arc::new(File::open(path).map_err(QueueError::Io)?);
    let stable = stable_file_path(&handle, path)?;
    Ok(PinnedDirectory {
        entry: path.into(),
        stable,
        handle,
    })
}

#[cfg(target_os = "linux")]
fn validate_common_queue_mount(
    root: &File,
    directories: &QueueDirectories,
) -> Result<(), QueueError> {
    let root_mount = linux_mount_id(root)?;
    for directory in directories.all() {
        if linux_mount_id(&directory.handle)? != root_mount {
            return Err(QueueError::InvalidStoragePath(directory.entry.clone()));
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn linux_mount_id(file: &File) -> Result<u64, QueueError> {
    use std::os::fd::AsRawFd;

    const MAXIMUM_FDINFO_BYTES: u64 = 16 * 1024;

    let path = PathBuf::from(format!("/proc/self/fdinfo/{}", file.as_raw_fd()));
    let mut bytes = Vec::with_capacity(MAXIMUM_FDINFO_BYTES as usize);
    File::open(path)
        .and_then(|file| file.take(MAXIMUM_FDINFO_BYTES + 1).read_to_end(&mut bytes))
        .map_err(QueueError::Io)?;
    if bytes.len() as u64 > MAXIMUM_FDINFO_BYTES {
        return Err(QueueError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "file descriptor metadata exceeds the supported size",
        )));
    }
    let contents = std::str::from_utf8(&bytes).map_err(|_| {
        QueueError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "file descriptor metadata is not UTF-8",
        ))
    })?;
    contents
        .lines()
        .find_map(|line| line.strip_prefix("mnt_id:").map(str::trim))
        .ok_or_else(|| {
            QueueError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "file descriptor metadata has no mount identifier",
            ))
        })?
        .parse()
        .map_err(|_| {
            QueueError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "file descriptor mount identifier is invalid",
            ))
        })
}

#[cfg(all(unix, not(target_os = "linux")))]
fn validate_common_queue_mount(
    root: &File,
    directories: &QueueDirectories,
) -> Result<(), QueueError> {
    use std::os::unix::fs::MetadataExt;

    let root_device = root.metadata().map_err(QueueError::Io)?.dev();
    for directory in directories.all() {
        if directory.handle.metadata().map_err(QueueError::Io)?.dev() != root_device {
            return Err(QueueError::InvalidStoragePath(directory.entry.clone()));
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_common_queue_mount(
    _root: &File,
    _directories: &QueueDirectories,
) -> Result<(), QueueError> {
    Ok(())
}

fn validate_pinned_directory(directory: &PinnedDirectory) -> Result<(), QueueError> {
    let entry = fs::symlink_metadata(&directory.entry).map_err(QueueError::Io)?;
    let pinned = directory.handle.metadata().map_err(QueueError::Io)?;
    if !entry.file_type().is_dir() {
        return Err(QueueError::InvalidQueueIdentity);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if entry.dev() != pinned.dev() || entry.ino() != pinned.ino() {
            return Err(QueueError::InvalidQueueIdentity);
        }
    }
    Ok(())
}

fn stable_file_path(handle: &File, _fallback: &Path) -> Result<PathBuf, QueueError> {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let stable = PathBuf::from(format!("/proc/self/fd/{}", handle.as_raw_fd()));
        fs::metadata(&stable).map_err(QueueError::Io)?;
        Ok(stable)
    }
    #[cfg(not(target_os = "linux"))]
    {
        Ok(_fallback.to_path_buf())
    }
}

fn read_queue_identity(path: &Path) -> Result<Revision, QueueError> {
    let metadata = fs::symlink_metadata(path).map_err(QueueError::Io)?;
    if !metadata.file_type().is_file() || metadata.len() > MAXIMUM_QUEUE_IDENTITY_BYTES {
        return Err(QueueError::InvalidQueueIdentity);
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)
        .and_then(|file| {
            file.take(MAXIMUM_QUEUE_IDENTITY_BYTES + 1)
                .read_to_end(&mut bytes)
        })
        .map_err(QueueError::Io)?;
    if bytes.len() as u64 > MAXIMUM_QUEUE_IDENTITY_BYTES {
        return Err(QueueError::InvalidQueueIdentity);
    }
    let value = std::str::from_utf8(&bytes)
        .map_err(|_| QueueError::InvalidQueueIdentity)?
        .trim();
    value.parse().map_err(|_| QueueError::InvalidQueueIdentity)
}

fn accepted_states_are_empty(queue_root: &Path) -> Result<bool, QueueError> {
    for state in QueueState::ALL {
        let mut entries =
            fs::read_dir(queue_root.join(state.directory_name())).map_err(QueueError::Io)?;
        if entries
            .next()
            .transpose()
            .map_err(QueueError::Io)?
            .is_some()
        {
            return Ok(false);
        }
    }
    Ok(true)
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

fn write_acceptance_file(
    package_root: &Path,
    acceptance: AcceptanceMetadata,
) -> Result<(), QueueError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(package_root.join(ACCEPTANCE_FILE_NAME))
        .map_err(QueueError::Io)?;
    serde_json::to_writer(&mut file, &acceptance).map_err(QueueError::AcceptanceMetadata)?;
    file.write_all(b"\n").map_err(QueueError::Io)
}

fn allocate_sequence(queue_root: &Path) -> Result<u64, QueueError> {
    let sequence_path = queue_root.join(NEXT_SEQUENCE_FILE_NAME);
    let sequence = read_next_sequence(&sequence_path)?;
    let next = sequence
        .checked_add(1)
        .ok_or(QueueError::SequenceExhausted)?;
    let temporary_path = queue_root.join(format!(".next-sequence-{}", Ulid::generate()));
    let mut temporary = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary_path)
        .map_err(QueueError::Io)?;
    if let Err(error) = writeln!(temporary, "{next}") {
        let _ = fs::remove_file(&temporary_path);
        return Err(QueueError::Io(error));
    }
    if let Err(error) = temporary.sync_all() {
        let _ = fs::remove_file(&temporary_path);
        return Err(QueueError::Io(error));
    }
    drop(temporary);
    if let Err(error) = fs::rename(&temporary_path, &sequence_path) {
        let _ = fs::remove_file(&temporary_path);
        return Err(QueueError::Io(error));
    }
    sync_directory(queue_root)?;
    Ok(sequence)
}

fn read_next_sequence(path: &Path) -> Result<u64, QueueError> {
    let mut bytes = Vec::with_capacity(MAXIMUM_SEQUENCE_FILE_BYTES as usize);
    File::open(path)
        .map_err(QueueError::Io)?
        .take(MAXIMUM_SEQUENCE_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(QueueError::Io)?;
    if bytes.len() as u64 > MAXIMUM_SEQUENCE_FILE_BYTES {
        return Err(QueueError::InvalidSequenceState);
    }
    let contents = std::str::from_utf8(&bytes).map_err(|_| QueueError::InvalidSequenceState)?;
    let Some(value) = contents.strip_suffix('\n') else {
        return Err(QueueError::InvalidSequenceState);
    };
    if value.is_empty()
        || value.starts_with('0')
        || value.contains('\n')
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(QueueError::InvalidSequenceState);
    }
    value.parse().map_err(|_| QueueError::InvalidSequenceState)
}

fn sync_package(package_root: &Path, package: &ValidatedPackage) -> Result<(), QueueError> {
    sync_file(&package_root.join(REQUEST_FILE_NAME))?;
    sync_file(&package_root.join(DIGEST_FILE_NAME))?;

    let payload_root = package_root.join(PAYLOAD_DIRECTORY_NAME);
    let mut directories = HashSet::new();
    directories.insert(payload_root.clone());
    for payload in package.payload() {
        sync_file(&payload_root.join(payload.path().as_str()))?;

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

fn sync_file(path: &Path) -> Result<(), QueueError> {
    File::open(path)
        .map_err(QueueError::Io)?
        .sync_all()
        .map_err(QueueError::Io)
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
    let digest_metadata = match fs::symlink_metadata(&digest_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(QueueError::CorruptState {
                request_id,
                state,
                detail: "digest is missing",
            });
        }
        Err(error) => return Err(QueueError::Io(error)),
    };
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
    AcceptanceMetadataSynchronized,
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
    /// Number of nested directories below `payload/`.
    DirectoryCount,
    /// Combined number of request, payload-file, and payload-directory entries.
    EntryCount,
    /// Number of components in one payload path.
    PathComponents,
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
    /// A payload file path collided with an existing payload directory.
    PayloadPrefixCollision(PayloadPath),
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
    /// Serializing Gateway-owned acceptance metadata failed.
    AcceptanceMetadata(serde_json::Error),
    /// Formatting the Gateway-owned quarantine timestamp failed.
    QuarantineTimestamp(time::error::Format),
    /// The durable queue acceptance sequence was exhausted.
    SequenceExhausted,
    /// The durable queue acceptance sequence file was malformed.
    InvalidSequenceState,
    /// The immutable queue instance identity was missing, malformed, or changed.
    InvalidQueueIdentity,
    /// An in-process maintenance scanner mutex was poisoned.
    MaintenanceScannerPoisoned,
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
            Self::RequestAlreadyWritten
            | Self::PayloadAlreadyWritten(_)
            | Self::PayloadPrefixCollision(_) => ErrorCode::InvalidRequest,
            Self::LimitExceeded { .. } => ErrorCode::LimitExceeded,
            Self::RequestIdReused { .. } => ErrorCode::RequestIdReused,
            Self::CorruptState { .. } => ErrorCode::ContentValidationFailed,
            Self::InvalidStoragePath(_)
            | Self::RequestInMultipleStates { .. }
            | Self::AcceptanceMetadata(_)
            | Self::QuarantineTimestamp(_)
            | Self::SequenceExhausted
            | Self::InvalidSequenceState
            | Self::InvalidQueueIdentity
            | Self::MaintenanceScannerPoisoned => ErrorCode::InternalError,
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
            Self::PayloadPrefixCollision(path) => {
                write!(
                    formatter,
                    "payload path `{path}` collides with an existing file or directory"
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
            Self::AcceptanceMetadata(error) => {
                write!(
                    formatter,
                    "acceptance metadata serialization failed: {error}"
                )
            }
            Self::QuarantineTimestamp(error) => {
                write!(formatter, "quarantine timestamp formatting failed: {error}")
            }
            Self::SequenceExhausted => {
                formatter.write_str("durable acceptance sequence is exhausted")
            }
            Self::InvalidSequenceState => {
                formatter.write_str("durable acceptance sequence state is invalid")
            }
            Self::InvalidQueueIdentity => {
                formatter.write_str("durable queue instance identity is invalid")
            }
            Self::MaintenanceScannerPoisoned => {
                formatter.write_str("maintenance scanner state is unavailable")
            }
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
            Self::AcceptanceMetadata(error) => Some(error),
            Self::QuarantineTimestamp(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests;

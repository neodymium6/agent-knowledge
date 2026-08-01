use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use agent_knowledge_core::{
    ErrorCode, PathAttestation, PathAttestationError, PinnedDirectory as SafeDirectory,
    PinnedPathError, PinnedRegularFile, RequestId, Revision,
};
use time::OffsetDateTime;

use super::{
    LOCK_DIRECTORY_NAME, PinnedDirectory, QUEUE_IDENTITY_FILE_NAME, QUEUE_LOCK_FILE_NAME,
    QueueBinding, QueueDirectories, QueueError, QueueState, WORKER_LOCK_FILE_NAME,
    WORKER_TEMP_DIRECTORY_NAME, stable_file_path, validate_common_queue_mount,
    validate_current_queue,
};
use crate::{CURRENT_WORKER_RESULT_SCHEMA_VERSION, WorkerResultRecord, WorkerResultStatus};

const RESULT_FILE_NAME: &str = "result.json";
const MAXIMUM_RESULT_FILE_BYTES: u64 = 1_024;
const MAXIMUM_STATE_OBSERVATION_ATTEMPTS: usize = 3;

pub(super) trait StatusObservationHook {
    fn after_state(&mut self, state: QueueState);
}

struct NoopStatusObservationHook;

impl StatusObservationHook for NoopStatusObservationHook {
    fn after_state(&mut self, _state: QueueState) {}
}

/// A request state observed without changing or locking the durable queue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueueRequestStatus {
    /// Accepted and awaiting the Repository Worker.
    Pending,
    /// Claimed by the Repository Worker.
    Processing,
    /// Committed and published locally.
    Completed,
    /// Permanently rejected with a durable failure result.
    Failed {
        /// Stable machine-readable failure classification.
        error_code: ErrorCode,
        /// Central-server time when the failure became durable.
        failed_at: OffsetDateTime,
    },
}

/// A pinned, read-only view of one initialized durable queue.
#[derive(Debug)]
pub struct QueueReader {
    queue_root: PathBuf,
    configured_queue_root: PathBuf,
    root_handle: Arc<File>,
    directories: Arc<QueueDirectories>,
    identity: Revision,
    lock_file: PathBuf,
    queue_lock_handle: Arc<File>,
    worker_lock_file: PathBuf,
    worker_lock_handle: Arc<File>,
}

impl QueueReader {
    /// Opens an existing queue without creating files, taking locks, or running
    /// maintenance.
    ///
    /// # Errors
    ///
    /// Returns an error when the queue is absent, structurally invalid,
    /// replaced during opening, or cannot be opened before `deadline`.
    pub fn open_until(
        queue_root: impl Into<PathBuf>,
        deadline: Option<Instant>,
    ) -> Result<Self, QueueError> {
        ensure_deadline(deadline)?;
        let configured_path = queue_root.into();
        let safe_root = SafeDirectory::open(&configured_path)
            .map_err(|error| opening_path_error(error, &configured_path))?;
        let root_handle = Arc::new(
            safe_root
                .try_clone_file()
                .map_err(|error| opening_path_error(error, &configured_path))?,
        );
        let configured_queue_root = fs::canonicalize(&configured_path).map_err(QueueError::Io)?;
        let queue_root = stable_file_path(&root_handle, &configured_queue_root)?;
        ensure_deadline(deadline)?;

        let directories = Arc::new(QueueDirectories {
            lock: pin_existing_directory(&queue_root.join(LOCK_DIRECTORY_NAME))?,
            incoming: pin_existing_directory(&queue_root.join("incoming"))?,
            quarantine: pin_existing_directory(&queue_root.join("quarantine"))?,
            worker_temporary: pin_existing_directory(&queue_root.join(WORKER_TEMP_DIRECTORY_NAME))?,
            states: [
                pin_existing_directory(&queue_root.join(QueueState::Pending.directory_name()))?,
                pin_existing_directory(&queue_root.join(QueueState::Processing.directory_name()))?,
                pin_existing_directory(&queue_root.join(QueueState::Completed.directory_name()))?,
                pin_existing_directory(&queue_root.join(QueueState::Failed.directory_name()))?,
            ],
        });
        validate_common_queue_mount(&root_handle, &directories)?;
        ensure_deadline(deadline)?;

        let lock_file = directories.lock.stable.join(QUEUE_LOCK_FILE_NAME);
        let queue_lock_handle = Arc::new(open_existing_regular(&lock_file)?);
        let worker_lock_file = directories.lock.stable.join(WORKER_LOCK_FILE_NAME);
        let worker_lock_handle = Arc::new(open_existing_regular(&worker_lock_file)?);
        let identity = super::read_queue_identity(&queue_root.join(QUEUE_IDENTITY_FILE_NAME))?;
        ensure_deadline(deadline)?;

        let reader = Self {
            queue_root,
            configured_queue_root,
            root_handle,
            directories,
            identity,
            lock_file,
            queue_lock_handle,
            worker_lock_file,
            worker_lock_handle,
        };
        reader.current_identity()?;
        ensure_deadline(deadline)?;
        Ok(reader)
    }

    /// Attests the queue root selected and pinned while opening.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured root no longer names the pinned
    /// queue object or its ancestry cannot be inspected.
    pub fn storage_attestation(&self) -> Result<PathAttestation, PathAttestationError> {
        PathAttestation::capture(&self.configured_queue_root, &self.root_handle)
    }

    /// Observes one request without taking the queue or Worker lock.
    ///
    /// A state transition may make the request briefly visible under both the
    /// source and destination names during an unlocked scan. Those observations
    /// are retried a fixed number of times before being classified as corrupt.
    ///
    /// # Errors
    ///
    /// Returns an error when queue identity changes, durable state is corrupt,
    /// I/O fails, or the deadline expires.
    pub fn status_until(
        &self,
        request_id: RequestId,
        deadline: Option<Instant>,
    ) -> Result<Option<QueueRequestStatus>, QueueError> {
        self.status_until_with_hook(request_id, deadline, &mut NoopStatusObservationHook)
    }

    pub(super) fn status_until_with_hook(
        &self,
        request_id: RequestId,
        deadline: Option<Instant>,
        hook: &mut dyn StatusObservationHook,
    ) -> Result<Option<QueueRequestStatus>, QueueError> {
        ensure_deadline(deadline)?;
        self.current_identity()?;
        for attempt in 0..MAXIMUM_STATE_OBSERVATION_ATTEMPTS {
            let observed = self.observe_states(request_id, deadline, hook)?;
            match observed.as_slice() {
                [] if attempt + 1 == MAXIMUM_STATE_OBSERVATION_ATTEMPTS => {
                    self.current_identity()?;
                    ensure_deadline(deadline)?;
                    return Ok(None);
                }
                [(state, directory)] => {
                    let status = match state {
                        QueueState::Pending => QueueRequestStatus::Pending,
                        QueueState::Processing => QueueRequestStatus::Processing,
                        QueueState::Completed => QueueRequestStatus::Completed,
                        QueueState::Failed => read_failed_status(directory, request_id)?,
                    };
                    self.current_identity()?;
                    ensure_deadline(deadline)?;
                    return Ok(Some(status));
                }
                [] | [_, _, ..] => ensure_deadline(deadline)?,
            }
        }
        Err(QueueError::RequestInMultipleStates { request_id })
    }

    fn observe_states(
        &self,
        request_id: RequestId,
        deadline: Option<Instant>,
        hook: &mut dyn StatusObservationHook,
    ) -> Result<Vec<(QueueState, SafeDirectory)>, QueueError> {
        let mut observed = Vec::with_capacity(2);
        for state in QueueState::ALL {
            ensure_deadline(deadline)?;
            let path = self
                .directories
                .state(state)
                .stable
                .join(request_id.to_string());
            match SafeDirectory::open(&path) {
                Ok(directory) => observed.push((state, directory)),
                Err(PinnedPathError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {}
                Err(PinnedPathError::ExpectedDirectory) => {
                    return Err(QueueError::CorruptState {
                        request_id,
                        state,
                        detail: "request entry is not a directory",
                    });
                }
                Err(PinnedPathError::Io(error)) => return Err(QueueError::Io(error)),
                Err(_) => return Err(QueueError::InvalidStoragePath(path)),
            }
            hook.after_state(state);
        }
        Ok(observed)
    }

    fn current_identity(&self) -> Result<Revision, QueueError> {
        validate_current_queue(QueueBinding {
            configured_queue_root: &self.configured_queue_root,
            queue_root: &self.queue_root,
            root_handle: &self.root_handle,
            directories: &self.directories,
            identity: self.identity,
            lock_file: &self.lock_file,
            queue_lock_handle: &self.queue_lock_handle,
            worker_lock_file: &self.worker_lock_file,
            worker_lock_handle: &self.worker_lock_handle,
        })
    }
}

fn read_failed_status(
    directory: &SafeDirectory,
    request_id: RequestId,
) -> Result<QueueRequestStatus, QueueError> {
    let mut file = match directory.open_regular_beneath(RESULT_FILE_NAME) {
        Ok(file) => file,
        Err(PinnedPathError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
            return Err(QueueError::CorruptState {
                request_id,
                state: QueueState::Failed,
                detail: "failed request is missing result metadata",
            });
        }
        Err(PinnedPathError::Io(error)) => return Err(QueueError::Io(error)),
        Err(_) => {
            return Err(QueueError::CorruptState {
                request_id,
                state: QueueState::Failed,
                detail: "failed request result metadata is not a regular file",
            });
        }
    };
    if file.byte_length() > MAXIMUM_RESULT_FILE_BYTES {
        return Err(QueueError::CorruptState {
            request_id,
            state: QueueState::Failed,
            detail: "failed request result metadata exceeds its byte limit",
        });
    }
    let mut bytes = Vec::with_capacity(file.byte_length() as usize);
    file.by_ref()
        .take(MAXIMUM_RESULT_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(QueueError::Io)?;
    if bytes.len() as u64 > MAXIMUM_RESULT_FILE_BYTES {
        return Err(QueueError::CorruptState {
            request_id,
            state: QueueState::Failed,
            detail: "failed request result metadata exceeds its byte limit",
        });
    }
    let record: WorkerResultRecord =
        serde_json::from_slice(&bytes).map_err(|_| QueueError::CorruptState {
            request_id,
            state: QueueState::Failed,
            detail: "failed request result metadata is invalid",
        })?;
    if record.schema_version != CURRENT_WORKER_RESULT_SCHEMA_VERSION
        || record.request_id != request_id
        || record.status != WorkerResultStatus::Failed
    {
        return Err(QueueError::CorruptState {
            request_id,
            state: QueueState::Failed,
            detail: "failed request result metadata does not match its queue entry",
        });
    }
    Ok(QueueRequestStatus::Failed {
        error_code: record.error_code,
        failed_at: record.failed_at,
    })
}

fn pin_existing_directory(path: &Path) -> Result<PinnedDirectory, QueueError> {
    let safe = SafeDirectory::open(path).map_err(|error| opening_path_error(error, path))?;
    let handle = Arc::new(
        safe.try_clone_file()
            .map_err(|error| opening_path_error(error, path))?,
    );
    let stable = stable_file_path(&handle, path)?;
    Ok(PinnedDirectory {
        entry: path.into(),
        stable,
        handle,
    })
}

fn open_existing_regular(path: &Path) -> Result<File, QueueError> {
    PinnedRegularFile::open_no_follow(path)
        .map_err(|error| match error {
            agent_knowledge_core::BoundedFileError::Io(error) => QueueError::Io(error),
            _ => QueueError::InvalidStoragePath(path.into()),
        })?
        .try_clone_file()
        .map_err(QueueError::Io)
}

fn opening_path_error(error: PinnedPathError, path: &Path) -> QueueError {
    match error {
        PinnedPathError::Io(error) => QueueError::Io(error),
        _ => QueueError::InvalidStoragePath(path.into()),
    }
}

fn ensure_deadline(deadline: Option<Instant>) -> Result<(), QueueError> {
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        Err(QueueError::OperationDeadlineExceeded)
    } else {
        Ok(())
    }
}

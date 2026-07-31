use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};

use agent_knowledge_core::{BatchId, ErrorCode, RequestId};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use ulid::Ulid;

use super::{FileQueue, QueueError, QueueState, WORKER_TEMP_DIRECTORY_NAME, sync_directory};
use crate::{PackageValidationError, ValidatedPackage, validate_accepted_package};

mod batch;
pub use batch::{BatchClaimOutcome, WorkerSession};
mod result;
pub use result::{CURRENT_WORKER_RESULT_SCHEMA_VERSION, WorkerResultRecord, WorkerResultStatus};
mod recovery;
pub use recovery::ProcessingScanOutcome;

const PHASE_FILE_NAME: &str = "phase.json";
const MAXIMUM_PHASE_FILE_BYTES: u64 = 1_024;

/// Current schema version for Repository Worker phase sidecars.
pub const CURRENT_WORKER_PHASE_SCHEMA_VERSION: u16 = 1;

/// A durable processing phase recorded inside an accepted package.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerPhase {
    /// The request has moved under one Worker's batch ownership.
    Claimed,
}

/// Durable ownership metadata for one processing request.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerPhaseRecord {
    /// Worker phase schema version.
    pub schema_version: u16,
    /// Request whose package contains this record.
    pub request_id: RequestId,
    /// Worker batch that owns the current attempt.
    pub batch_id: BatchId,
    /// One-based processing attempt.
    pub attempt: NonZeroU32,
    /// Last durable processing phase.
    pub phase: WorkerPhase,
    /// Central-server time when this phase was recorded.
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

/// Exact ownership precondition required to mutate a claimed request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClaimToken {
    request_id: RequestId,
    batch_id: BatchId,
    attempt: NonZeroU32,
}

impl ClaimToken {
    /// Returns the claimed request identifier.
    #[must_use]
    pub const fn request_id(self) -> RequestId {
        self.request_id
    }

    /// Returns the owning Worker batch.
    #[must_use]
    pub const fn batch_id(self) -> BatchId {
        self.batch_id
    }

    /// Returns the one-based processing attempt.
    #[must_use]
    pub const fn attempt(self) -> NonZeroU32 {
        self.attempt
    }
}

/// A validated package atomically moved from `pending/` to `processing/`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimedPackage {
    token: ClaimToken,
    package: ValidatedPackage,
}

impl ClaimedPackage {
    /// Returns the ownership token for later phase transitions.
    #[must_use]
    pub const fn token(&self) -> ClaimToken {
        self.token
    }

    /// Returns the revalidated immutable request package.
    #[must_use]
    pub const fn package(&self) -> &ValidatedPackage {
        &self.package
    }
}

pub(super) struct PreparedClaim {
    package: ValidatedPackage,
}

impl FileQueue {
    /// Atomically claims one pending request for a Worker batch.
    ///
    /// A successful return guarantees that a synchronized `phase.json` names
    /// the returned batch and attempt, the package is in `processing/`, and
    /// both queue-state directories have been synchronized.
    ///
    /// # Errors
    ///
    /// Returns an error when the request is absent, is not pending, contains
    /// corrupt immutable data or phase metadata, exhausts its attempt counter,
    /// or cannot be durably moved.
    fn claim(
        &self,
        request_id: RequestId,
        batch_id: BatchId,
    ) -> Result<ClaimedPackage, WorkerQueueError> {
        self.claim_with_hook(request_id, batch_id, &mut NoopClaimHook)
    }

    fn claim_with_hook(
        &self,
        request_id: RequestId,
        batch_id: BatchId,
        hook: &mut dyn ClaimHook,
    ) -> Result<ClaimedPackage, WorkerQueueError> {
        let lock = self.open_queue_lock().map_err(WorkerQueueError::Queue)?;
        lock.lock().map_err(WorkerQueueError::Io)?;
        self.ensure_pending_locked(request_id)?;
        drop(lock);

        let prepared = match self.prepare_claim(request_id) {
            Ok(prepared) => prepared,
            Err(error) => {
                let lock = self.open_queue_lock().map_err(WorkerQueueError::Queue)?;
                lock.lock().map_err(WorkerQueueError::Io)?;
                match self.ensure_pending_locked(request_id) {
                    Ok(_) => return Err(error),
                    Err(state_error) => return Err(state_error),
                }
            }
        };
        self.claim_prepared_with_hook(request_id, batch_id, prepared, hook)
    }

    pub(super) fn prepare_claim(
        &self,
        request_id: RequestId,
    ) -> Result<PreparedClaim, WorkerQueueError> {
        let pending_path = self.state_path(QueueState::Pending, request_id);
        let package =
            revalidate_accepted(&pending_path, request_id, QueueState::Pending, &self.policy)?;
        if package.request().request_id != request_id {
            return Err(WorkerQueueError::CorruptState {
                request_id,
                state: QueueState::Pending,
                detail: "accepted package identity does not match its queue entry",
            });
        }
        let _ = next_attempt(&pending_path, request_id, QueueState::Pending)?;
        Ok(PreparedClaim { package })
    }

    pub(super) fn claim_prepared(
        &self,
        request_id: RequestId,
        batch_id: BatchId,
        prepared: PreparedClaim,
    ) -> Result<ClaimedPackage, WorkerQueueError> {
        self.claim_prepared_with_hook(request_id, batch_id, prepared, &mut NoopClaimHook)
    }

    fn claim_prepared_with_hook(
        &self,
        request_id: RequestId,
        batch_id: BatchId,
        prepared: PreparedClaim,
        hook: &mut dyn ClaimHook,
    ) -> Result<ClaimedPackage, WorkerQueueError> {
        let lock = self.open_queue_lock().map_err(WorkerQueueError::Queue)?;
        lock.lock().map_err(WorkerQueueError::Io)?;
        let stored_digest = self.ensure_pending_locked(request_id)?;
        if stored_digest != prepared.package.digest() {
            return Err(WorkerQueueError::CorruptState {
                request_id,
                state: QueueState::Pending,
                detail: "accepted package changed after Worker validation",
            });
        }

        let pending_path = self.state_path(QueueState::Pending, request_id);
        let attempt = next_attempt(&pending_path, request_id, QueueState::Pending)?;
        let record = WorkerPhaseRecord {
            schema_version: CURRENT_WORKER_PHASE_SCHEMA_VERSION,
            request_id,
            batch_id,
            attempt,
            phase: WorkerPhase::Claimed,
            updated_at: OffsetDateTime::now_utc(),
        };
        write_phase_record(self, &pending_path, &record)?;
        hook.reached(ClaimPhase::PhaseSynchronized)
            .map_err(WorkerQueueError::Io)?;

        let processing_path = self.state_path(QueueState::Processing, request_id);
        fs::rename(&pending_path, &processing_path).map_err(WorkerQueueError::Io)?;
        hook.reached(ClaimPhase::Renamed)
            .map_err(WorkerQueueError::Io)?;

        sync_directory(
            &self
                .queue_root
                .join(QueueState::Processing.directory_name()),
        )
        .map_err(WorkerQueueError::Queue)?;
        sync_directory(&self.queue_root.join(QueueState::Pending.directory_name()))
            .map_err(WorkerQueueError::Queue)?;
        hook.reached(ClaimPhase::QueueDirectoriesSynchronized)
            .map_err(WorkerQueueError::Io)?;

        Ok(ClaimedPackage {
            token: ClaimToken {
                request_id,
                batch_id,
                attempt,
            },
            package: prepared.package,
        })
    }

    fn ensure_pending_locked(
        &self,
        request_id: RequestId,
    ) -> Result<crate::PackageDigest, WorkerQueueError> {
        let Some((state, digest)) = self
            .find_existing(request_id)
            .map_err(WorkerQueueError::Queue)?
        else {
            return Err(WorkerQueueError::RequestNotFound { request_id });
        };
        if state != QueueState::Pending {
            return Err(WorkerQueueError::InvalidState {
                request_id,
                expected: QueueState::Pending,
                actual: state,
            });
        }
        Ok(digest)
    }

    /// Returns one still-claimed processing request to `pending/`.
    ///
    /// The complete claim token is checked so a stale Worker cannot requeue a
    /// later attempt owned by another batch.
    ///
    /// # Errors
    ///
    /// Returns an error when the request is absent, is not processing, no
    /// longer matches the token, has corrupt phase metadata, or cannot be
    /// durably moved.
    fn requeue_claimed(&self, token: ClaimToken) -> Result<(), WorkerQueueError> {
        let lock = self.open_queue_lock().map_err(WorkerQueueError::Queue)?;
        lock.lock().map_err(WorkerQueueError::Io)?;

        let request_id = token.request_id;
        let Some((state, _)) = self
            .find_existing(request_id)
            .map_err(WorkerQueueError::Queue)?
        else {
            return Err(WorkerQueueError::RequestNotFound { request_id });
        };
        if state != QueueState::Processing {
            return Err(WorkerQueueError::InvalidState {
                request_id,
                expected: QueueState::Processing,
                actual: state,
            });
        }
        let processing_path = self.state_path(QueueState::Processing, request_id);
        let record = read_required_phase_record(&processing_path, request_id, state)?;
        if record.batch_id != token.batch_id
            || record.attempt != token.attempt
            || record.phase != WorkerPhase::Claimed
        {
            return Err(WorkerQueueError::ClaimChanged { request_id });
        }

        let pending_path = self.state_path(QueueState::Pending, request_id);
        fs::rename(&processing_path, pending_path).map_err(WorkerQueueError::Io)?;
        sync_directory(&self.queue_root.join(QueueState::Pending.directory_name()))
            .map_err(WorkerQueueError::Queue)?;
        sync_directory(
            &self
                .queue_root
                .join(QueueState::Processing.directory_name()),
        )
        .map_err(WorkerQueueError::Queue)
    }
}

fn revalidate_accepted(
    package_root: &Path,
    request_id: RequestId,
    state: QueueState,
    policy: &crate::PackagePolicy,
) -> Result<ValidatedPackage, WorkerQueueError> {
    validate_accepted_package(package_root, policy).map_err(|error| match error {
        PackageValidationError::Io(error) => WorkerQueueError::Io(error),
        error => WorkerQueueError::CorruptPackage {
            request_id,
            state,
            source: error,
        },
    })
}

fn next_attempt(
    package_root: &Path,
    request_id: RequestId,
    state: QueueState,
) -> Result<NonZeroU32, WorkerQueueError> {
    let previous = read_optional_phase_record(package_root, request_id, state)?
        .map_or(0, |record| record.attempt.get());
    let next = previous
        .checked_add(1)
        .ok_or(WorkerQueueError::AttemptExhausted { request_id })?;
    NonZeroU32::new(next).ok_or(WorkerQueueError::AttemptExhausted { request_id })
}

fn read_optional_phase_record(
    package_root: &Path,
    request_id: RequestId,
    state: QueueState,
) -> Result<Option<WorkerPhaseRecord>, WorkerQueueError> {
    let path = package_root.join(PHASE_FILE_NAME);
    match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_file() => {
            read_phase_record(&path, request_id, state).map(Some)
        }
        Ok(_) => Err(WorkerQueueError::CorruptState {
            request_id,
            state,
            detail: "phase metadata is not a regular file",
        }),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(WorkerQueueError::Io(error)),
    }
}

fn read_required_phase_record(
    package_root: &Path,
    request_id: RequestId,
    state: QueueState,
) -> Result<WorkerPhaseRecord, WorkerQueueError> {
    read_optional_phase_record(package_root, request_id, state)?.ok_or(
        WorkerQueueError::CorruptState {
            request_id,
            state,
            detail: "processing package is missing phase metadata",
        },
    )
}

fn read_phase_record(
    path: &Path,
    request_id: RequestId,
    state: QueueState,
) -> Result<WorkerPhaseRecord, WorkerQueueError> {
    let mut bytes = Vec::with_capacity(MAXIMUM_PHASE_FILE_BYTES as usize);
    File::open(path)
        .map_err(WorkerQueueError::Io)?
        .take(MAXIMUM_PHASE_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(WorkerQueueError::Io)?;
    if bytes.len() as u64 > MAXIMUM_PHASE_FILE_BYTES {
        return Err(WorkerQueueError::CorruptState {
            request_id,
            state,
            detail: "phase metadata exceeds its byte limit",
        });
    }
    let record: WorkerPhaseRecord =
        serde_json::from_slice(&bytes).map_err(WorkerQueueError::InvalidPhaseMetadata)?;
    if record.schema_version != CURRENT_WORKER_PHASE_SCHEMA_VERSION {
        return Err(WorkerQueueError::CorruptState {
            request_id,
            state,
            detail: "phase metadata has an unsupported schema version",
        });
    }
    if record.request_id != request_id {
        return Err(WorkerQueueError::CorruptState {
            request_id,
            state,
            detail: "phase metadata names a different request",
        });
    }
    Ok(record)
}

fn write_phase_record(
    queue: &FileQueue,
    package_root: &Path,
    record: &WorkerPhaseRecord,
) -> Result<(), WorkerQueueError> {
    let temporary_root = queue.queue_root.join(WORKER_TEMP_DIRECTORY_NAME);
    let temporary_path = temporary_root.join(format!(".phase-{}", Ulid::generate()));
    let destination = package_root.join(PHASE_FILE_NAME);
    match fs::symlink_metadata(&destination) {
        Ok(metadata) if metadata.file_type().is_file() => {}
        Ok(_) => {
            return Err(WorkerQueueError::CorruptState {
                request_id: record.request_id,
                state: QueueState::Pending,
                detail: "phase metadata is not a regular file",
            });
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(WorkerQueueError::Io(error)),
    }

    let result = (|| {
        let mut temporary = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary_path)
            .map_err(WorkerQueueError::Io)?;
        serde_json::to_writer(&mut temporary, record).map_err(WorkerQueueError::PhaseEncoding)?;
        temporary.write_all(b"\n").map_err(WorkerQueueError::Io)?;
        temporary.sync_all().map_err(WorkerQueueError::Io)?;
        drop(temporary);
        fs::rename(&temporary_path, destination).map_err(WorkerQueueError::Io)?;
        sync_directory(package_root).map_err(WorkerQueueError::Queue)?;
        sync_directory(&temporary_root).map_err(WorkerQueueError::Queue)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary_path);
    }
    result
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClaimPhase {
    PhaseSynchronized,
    Renamed,
    QueueDirectoriesSynchronized,
}

trait ClaimHook {
    fn reached(&mut self, phase: ClaimPhase) -> io::Result<()>;
}

struct NoopClaimHook;

impl ClaimHook for NoopClaimHook {
    fn reached(&mut self, _phase: ClaimPhase) -> io::Result<()> {
        Ok(())
    }
}

/// Failure while claiming or requeueing a Worker request.
#[derive(Debug)]
pub enum WorkerQueueError {
    /// A shared queue operation failed.
    Queue(QueueError),
    /// A file-system operation failed.
    Io(io::Error),
    /// Worker-owned phase JSON could not be encoded.
    PhaseEncoding(serde_json::Error),
    /// Worker-owned terminal result JSON could not be encoded.
    ResultEncoding(serde_json::Error),
    /// Stored Worker phase JSON was malformed.
    InvalidPhaseMetadata(serde_json::Error),
    /// Another process currently owns the Repository Worker lock.
    WorkerAlreadyRunning,
    /// A batch scan limit was zero.
    InvalidBatchLimits,
    /// A caller changed the identity or capacity of an active batch scan.
    BatchScanChanged {
        /// Batch identifier that owns the active scan.
        active_batch_id: BatchId,
    },
    /// Another Worker transition was requested during a pending batch scan.
    BatchScanInProgress {
        /// Batch identifier that owns the active scan.
        active_batch_id: BatchId,
    },
    /// A processing recovery scan was active during another transition.
    ProcessingScanInProgress,
    /// A new Worker session has not completed processing recovery.
    ProcessingRecoveryRequired,
    /// A processing recovery scan limit was zero.
    InvalidProcessingScanLimit,
    /// A pending directory entry did not have the required queue shape.
    InvalidPendingEntry {
        /// Invalid entry path.
        path: PathBuf,
        /// Non-sensitive diagnostic.
        detail: &'static str,
    },
    /// A processing directory entry did not have the required queue shape.
    InvalidProcessingEntry {
        /// Invalid entry path.
        path: PathBuf,
        /// Non-sensitive diagnostic.
        detail: &'static str,
    },
    /// No accepted request exists with this ID.
    RequestNotFound {
        /// Missing request identifier.
        request_id: RequestId,
    },
    /// The request exists in a state that does not permit the operation.
    InvalidState {
        /// Affected request identifier.
        request_id: RequestId,
        /// State required by the operation.
        expected: QueueState,
        /// Actual current state.
        actual: QueueState,
    },
    /// Immutable accepted package validation failed.
    CorruptPackage {
        /// Affected request identifier.
        request_id: RequestId,
        /// State containing the corrupt package.
        state: QueueState,
        /// Concrete package validation failure.
        source: PackageValidationError,
    },
    /// Queue-owned metadata was internally inconsistent.
    CorruptState {
        /// Affected request identifier.
        request_id: RequestId,
        /// State containing the corrupt package.
        state: QueueState,
        /// Non-sensitive diagnostic.
        detail: &'static str,
    },
    /// The one-based attempt counter cannot advance.
    AttemptExhausted {
        /// Affected request identifier.
        request_id: RequestId,
    },
    /// The claim token no longer names the durable owner and attempt.
    ClaimChanged {
        /// Affected request identifier.
        request_id: RequestId,
    },
}

impl WorkerQueueError {
    /// Returns a stable machine-readable classification.
    #[must_use]
    pub const fn error_code(&self) -> ErrorCode {
        match self {
            Self::Queue(error) => error.error_code(),
            Self::Io(_) | Self::WorkerAlreadyRunning => ErrorCode::TemporaryFailure,
            Self::RequestNotFound { .. }
            | Self::InvalidState { .. }
            | Self::ClaimChanged { .. }
            | Self::InvalidBatchLimits
            | Self::BatchScanChanged { .. }
            | Self::BatchScanInProgress { .. }
            | Self::ProcessingScanInProgress
            | Self::ProcessingRecoveryRequired
            | Self::InvalidProcessingScanLimit => ErrorCode::InvalidRequest,
            Self::CorruptPackage { .. }
            | Self::CorruptState { .. }
            | Self::InvalidPendingEntry { .. }
            | Self::InvalidProcessingEntry { .. }
            | Self::InvalidPhaseMetadata(_) => ErrorCode::ContentValidationFailed,
            Self::PhaseEncoding(_) | Self::ResultEncoding(_) | Self::AttemptExhausted { .. } => {
                ErrorCode::InternalError
            }
        }
    }
}

impl fmt::Display for WorkerQueueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Queue(error) => write!(formatter, "queue operation failed: {error}"),
            Self::Io(error) => write!(formatter, "Worker queue I/O failed: {error}"),
            Self::PhaseEncoding(error) => {
                write!(formatter, "phase metadata JSON encoding failed: {error}")
            }
            Self::ResultEncoding(error) => {
                write!(formatter, "result metadata JSON encoding failed: {error}")
            }
            Self::InvalidPhaseMetadata(error) => {
                write!(formatter, "stored phase metadata JSON is invalid: {error}")
            }
            Self::WorkerAlreadyRunning => {
                formatter.write_str("another Repository Worker owns the writer lock")
            }
            Self::InvalidBatchLimits => {
                formatter.write_str("batch scan and request limits must be greater than zero")
            }
            Self::BatchScanChanged { active_batch_id } => write!(
                formatter,
                "batch scan for `{active_batch_id}` must finish before its parameters change"
            ),
            Self::BatchScanInProgress { active_batch_id } => write!(
                formatter,
                "batch scan for `{active_batch_id}` must finish before another Worker transition"
            ),
            Self::ProcessingScanInProgress => formatter
                .write_str("processing recovery scan must finish before another transition"),
            Self::ProcessingRecoveryRequired => {
                formatter.write_str("processing recovery must complete before Worker transitions")
            }
            Self::InvalidProcessingScanLimit => {
                formatter.write_str("processing recovery scan limit must be greater than zero")
            }
            Self::InvalidPendingEntry { path, detail } => write!(
                formatter,
                "pending queue entry `{}` is invalid: {detail}",
                path.display()
            ),
            Self::InvalidProcessingEntry { path, detail } => write!(
                formatter,
                "processing queue entry `{}` is invalid: {detail}",
                path.display()
            ),
            Self::RequestNotFound { request_id } => {
                write!(formatter, "request `{request_id}` was not found")
            }
            Self::InvalidState {
                request_id,
                expected,
                actual,
            } => write!(
                formatter,
                "request `{request_id}` must be in `{expected}`, not `{actual}`"
            ),
            Self::CorruptPackage {
                request_id,
                state,
                source,
            } => write!(
                formatter,
                "request `{request_id}` in `{state}` failed immutable validation: {source}"
            ),
            Self::CorruptState {
                request_id,
                state,
                detail,
            } => write!(
                formatter,
                "request `{request_id}` in `{state}` has corrupt Worker state: {detail}"
            ),
            Self::AttemptExhausted { request_id } => {
                write!(
                    formatter,
                    "request `{request_id}` exhausted its attempt counter"
                )
            }
            Self::ClaimChanged { request_id } => {
                write!(
                    formatter,
                    "request `{request_id}` is owned by a different claim"
                )
            }
        }
    }
}

impl std::error::Error for WorkerQueueError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Queue(error) => Some(error),
            Self::Io(error) => Some(error),
            Self::PhaseEncoding(error)
            | Self::ResultEncoding(error)
            | Self::InvalidPhaseMetadata(error) => Some(error),
            Self::CorruptPackage { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests;

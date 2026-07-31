use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};

use agent_knowledge_core::{BatchId, ErrorCode, RequestId};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use ulid::Ulid;

use super::{ClaimToken, WorkerQueueError, read_required_phase_record};
use crate::file_queue::{
    FileQueue, QueueError, QueueState, WORKER_TEMP_DIRECTORY_NAME, sync_directory,
};

const RESULT_FILE_NAME: &str = "result.json";
const MAXIMUM_RESULT_FILE_BYTES: u64 = 1_024;

/// Current schema version for Repository Worker result sidecars.
pub const CURRENT_WORKER_RESULT_SCHEMA_VERSION: u16 = 1;

/// Terminal request status recorded by the Repository Worker.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerResultStatus {
    /// The request failed deterministic processing and will not be retried.
    Failed,
}

/// Durable terminal result for one request.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerResultRecord {
    /// Worker result schema version.
    pub schema_version: u16,
    /// Request whose package contains this record.
    pub request_id: RequestId,
    /// Batch that classified the terminal result.
    pub batch_id: BatchId,
    /// Terminal request status.
    pub status: WorkerResultStatus,
    /// Stable machine-readable failure classification.
    pub error_code: ErrorCode,
    /// Central-server time when failure was recorded.
    #[serde(with = "time::serde::rfc3339")]
    pub failed_at: OffsetDateTime,
}

impl FileQueue {
    pub(super) fn fail_pending(
        &self,
        request_id: RequestId,
        batch_id: BatchId,
        error_code: ErrorCode,
    ) -> Result<(), WorkerQueueError> {
        let queue_lock = self.open_queue_lock().map_err(WorkerQueueError::Queue)?;
        queue_lock.lock().map_err(WorkerQueueError::Io)?;

        let Some(state) = find_existing_state_without_package_read(self, request_id)? else {
            return Err(WorkerQueueError::RequestNotFound { request_id });
        };
        if state != QueueState::Pending {
            return Err(WorkerQueueError::InvalidState {
                request_id,
                expected: QueueState::Pending,
                actual: state,
            });
        }

        let pending_path = self.state_path(QueueState::Pending, request_id);
        let record = WorkerResultRecord {
            schema_version: CURRENT_WORKER_RESULT_SCHEMA_VERSION,
            request_id,
            batch_id,
            status: WorkerResultStatus::Failed,
            error_code,
            failed_at: OffsetDateTime::now_utc(),
        };
        write_result_record(self, &pending_path, QueueState::Pending, &record)?;

        let failed_path = self.state_path(QueueState::Failed, request_id);
        fs::rename(&pending_path, failed_path).map_err(WorkerQueueError::Io)?;
        sync_directory(&self.queue_root.join(QueueState::Failed.directory_name()))
            .map_err(WorkerQueueError::Queue)?;
        sync_directory(&self.queue_root.join(QueueState::Pending.directory_name()))
            .map_err(WorkerQueueError::Queue)
    }

    pub(super) fn complete_claimed(&self, token: ClaimToken) -> Result<(), WorkerQueueError> {
        let queue_lock = self.open_queue_lock().map_err(WorkerQueueError::Queue)?;
        queue_lock.lock().map_err(WorkerQueueError::Io)?;
        let request_id = token.request_id();
        let Some(state) = find_existing_state_without_package_read(self, request_id)? else {
            return Err(WorkerQueueError::RequestNotFound { request_id });
        };
        match state {
            QueueState::Processing => {
                validate_claim_token(self, token, state)?;
                transition(
                    self,
                    request_id,
                    QueueState::Processing,
                    QueueState::Completed,
                )
            }
            QueueState::Completed => {
                validate_claim_token(self, token, state)?;
                sync_transition_directories(self, QueueState::Processing, state)
            }
            actual => Err(WorkerQueueError::InvalidState {
                request_id,
                expected: QueueState::Processing,
                actual,
            }),
        }
    }

    pub(super) fn fail_claimed(
        &self,
        token: ClaimToken,
        error_code: ErrorCode,
    ) -> Result<(), WorkerQueueError> {
        let queue_lock = self.open_queue_lock().map_err(WorkerQueueError::Queue)?;
        queue_lock.lock().map_err(WorkerQueueError::Io)?;
        let request_id = token.request_id();
        let Some(state) = find_existing_state_without_package_read(self, request_id)? else {
            return Err(WorkerQueueError::RequestNotFound { request_id });
        };
        match state {
            QueueState::Processing => {
                validate_claim_token(self, token, state)?;
                let processing_path = self.state_path(state, request_id);
                let record = WorkerResultRecord {
                    schema_version: CURRENT_WORKER_RESULT_SCHEMA_VERSION,
                    request_id,
                    batch_id: token.batch_id(),
                    status: WorkerResultStatus::Failed,
                    error_code,
                    failed_at: OffsetDateTime::now_utc(),
                };
                write_result_record(self, &processing_path, state, &record)?;
                transition(self, request_id, state, QueueState::Failed)
            }
            QueueState::Failed => {
                validate_claim_token(self, token, state)?;
                let record = read_result_record(
                    &self.state_path(state, request_id).join(RESULT_FILE_NAME),
                    request_id,
                    state,
                )?;
                if record.batch_id == token.batch_id()
                    && record.status == WorkerResultStatus::Failed
                    && record.error_code == error_code
                {
                    sync_transition_directories(self, QueueState::Processing, state)
                } else {
                    Err(WorkerQueueError::ClaimChanged { request_id })
                }
            }
            actual => Err(WorkerQueueError::InvalidState {
                request_id,
                expected: QueueState::Processing,
                actual,
            }),
        }
    }
}

fn validate_claim_token(
    queue: &FileQueue,
    token: ClaimToken,
    state: QueueState,
) -> Result<(), WorkerQueueError> {
    let request_id = token.request_id();
    let record =
        read_required_phase_record(&queue.state_path(state, request_id), request_id, state)?;
    if record.batch_id == token.batch_id() && record.attempt == token.attempt() {
        Ok(())
    } else {
        Err(WorkerQueueError::ClaimChanged { request_id })
    }
}

fn transition(
    queue: &FileQueue,
    request_id: RequestId,
    source: QueueState,
    destination: QueueState,
) -> Result<(), WorkerQueueError> {
    fs::rename(
        queue.state_path(source, request_id),
        queue.state_path(destination, request_id),
    )
    .map_err(WorkerQueueError::Io)?;
    sync_transition_directories(queue, source, destination)
}

fn sync_transition_directories(
    queue: &FileQueue,
    source: QueueState,
    destination: QueueState,
) -> Result<(), WorkerQueueError> {
    sync_directory(&queue.queue_root.join(destination.directory_name()))
        .map_err(WorkerQueueError::Queue)?;
    sync_directory(&queue.queue_root.join(source.directory_name())).map_err(WorkerQueueError::Queue)
}

fn find_existing_state_without_package_read(
    queue: &FileQueue,
    request_id: RequestId,
) -> Result<Option<QueueState>, WorkerQueueError> {
    let mut existing = None;
    for state in QueueState::ALL {
        let path = queue.state_path(state, request_id);
        match fs::symlink_metadata(&path) {
            Ok(metadata) => {
                if !metadata.file_type().is_dir() {
                    return Err(WorkerQueueError::CorruptState {
                        request_id,
                        state,
                        detail: "request entry is not a directory",
                    });
                }
                if existing.is_some() {
                    return Err(WorkerQueueError::Queue(
                        QueueError::RequestInMultipleStates { request_id },
                    ));
                }
                existing = Some(state);
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(WorkerQueueError::Io(error)),
        }
    }
    Ok(existing)
}

fn write_result_record(
    queue: &FileQueue,
    package_root: &std::path::Path,
    state: QueueState,
    record: &WorkerResultRecord,
) -> Result<(), WorkerQueueError> {
    let temporary_root = queue.queue_root.join(WORKER_TEMP_DIRECTORY_NAME);
    let temporary_path = temporary_root.join(format!(".result-{}", Ulid::generate()));
    let destination = package_root.join(RESULT_FILE_NAME);
    match fs::symlink_metadata(&destination) {
        Ok(metadata) if metadata.file_type().is_file() => {}
        Ok(_) => {
            return Err(WorkerQueueError::CorruptState {
                request_id: record.request_id,
                state,
                detail: "result metadata is not a regular file",
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
        serde_json::to_writer(&mut temporary, record).map_err(WorkerQueueError::ResultEncoding)?;
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

fn read_result_record(
    path: &std::path::Path,
    request_id: RequestId,
    state: QueueState,
) -> Result<WorkerResultRecord, WorkerQueueError> {
    let mut bytes = Vec::with_capacity(MAXIMUM_RESULT_FILE_BYTES as usize);
    File::open(path)
        .map_err(WorkerQueueError::Io)?
        .take(MAXIMUM_RESULT_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(WorkerQueueError::Io)?;
    if bytes.len() as u64 > MAXIMUM_RESULT_FILE_BYTES {
        return Err(WorkerQueueError::CorruptState {
            request_id,
            state,
            detail: "result metadata exceeds its byte limit",
        });
    }
    let record: WorkerResultRecord =
        serde_json::from_slice(&bytes).map_err(WorkerQueueError::InvalidResultMetadata)?;
    if record.schema_version != CURRENT_WORKER_RESULT_SCHEMA_VERSION
        || record.request_id != request_id
    {
        return Err(WorkerQueueError::CorruptState {
            request_id,
            state,
            detail: "result metadata identity or schema is invalid",
        });
    }
    Ok(record)
}

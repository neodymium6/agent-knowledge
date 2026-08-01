use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};

use agent_knowledge_core::{BatchId, ErrorCode, RequestId, Revision};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use ulid::Ulid;

use super::{ClaimToken, WorkerPhase, WorkerQueueError, WorkerSession, read_required_phase_record};
use crate::file_queue::{FileQueue, QueueError, QueueState, sync_directory};

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

/// Opaque evidence that every request in one terminal batch was durably
/// reconciled with the queue.
#[derive(Debug)]
pub struct BatchReconciliation {
    queue_identity: Revision,
    batch_id: BatchId,
    outcomes: Vec<ReconciledOutcome>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReconciledOutcome {
    Completed(ClaimToken),
    Failed(ClaimToken, ErrorCode),
}

impl BatchReconciliation {
    /// Verifies that this proof covers exactly the expected durable outcomes.
    #[must_use]
    pub fn validates(
        &self,
        queue_identity: Revision,
        batch_id: BatchId,
        successful: &[ClaimToken],
        failures: &[(ClaimToken, ErrorCode)],
    ) -> bool {
        if self.queue_identity != queue_identity || self.batch_id != batch_id {
            return false;
        }
        let mut expected = successful
            .iter()
            .copied()
            .map(ReconciledOutcome::Completed)
            .chain(
                failures
                    .iter()
                    .copied()
                    .map(|(token, code)| ReconciledOutcome::Failed(token, code)),
            )
            .collect::<Vec<_>>();
        sort_outcomes(&mut expected);
        expected == self.outcomes
    }
}

impl WorkerSession {
    /// Idempotently moves all successful and permanently failed claims to
    /// terminal queue states and returns an opaque proof of the exact result.
    ///
    /// # Errors
    ///
    /// Returns an error when outcomes are empty, duplicated, span batches, no
    /// longer match their durable claims, or cannot be synchronized.
    pub fn reconcile_batch(
        &mut self,
        batch_id: BatchId,
        successful: &[ClaimToken],
        failures: &[(ClaimToken, ErrorCode)],
    ) -> Result<BatchReconciliation, WorkerQueueError> {
        self.queue_identity()?;
        self.ensure_no_active_scan()?;
        let mut request_ids = HashSet::with_capacity(successful.len() + failures.len());
        let valid = !successful.is_empty() || !failures.is_empty();
        let valid = valid
            && successful
                .iter()
                .chain(failures.iter().map(|(token, _)| token))
                .all(|token| {
                    token.batch_id() == batch_id && request_ids.insert(token.request_id())
                });
        if !valid {
            return Err(WorkerQueueError::InvalidReconciliation);
        }

        let mut outcomes = Vec::with_capacity(request_ids.len());
        for token in successful {
            self.queue.complete_claimed(*token)?;
            outcomes.push(ReconciledOutcome::Completed(*token));
        }
        for (token, error_code) in failures {
            self.queue.fail_claimed(*token, *error_code)?;
            outcomes.push(ReconciledOutcome::Failed(*token, *error_code));
        }
        sort_outcomes(&mut outcomes);
        let queue_identity = self.queue_identity()?;
        Ok(BatchReconciliation {
            queue_identity,
            batch_id,
            outcomes,
        })
    }
}

impl FileQueue {
    fn complete_claimed(&self, token: ClaimToken) -> Result<(), WorkerQueueError> {
        self.transition_claimed(token, QueueState::Completed, None)
    }

    fn fail_claimed(
        &self,
        token: ClaimToken,
        error_code: ErrorCode,
    ) -> Result<(), WorkerQueueError> {
        self.transition_claimed(token, QueueState::Failed, Some(error_code))
    }

    fn transition_claimed(
        &self,
        token: ClaimToken,
        terminal_state: QueueState,
        error_code: Option<ErrorCode>,
    ) -> Result<(), WorkerQueueError> {
        let queue_lock = self.open_queue_lock().map_err(WorkerQueueError::Queue)?;
        queue_lock.lock().map_err(WorkerQueueError::Io)?;
        self.current_identity_locked()
            .map_err(WorkerQueueError::Queue)?;
        let request_id = token.request_id();
        let Some(state) = find_existing_state_without_package_read(self, request_id)? else {
            return Err(WorkerQueueError::RequestNotFound { request_id });
        };
        if state == terminal_state {
            validate_terminal_claim(self, token, state, error_code)?;
            return Ok(());
        }
        if state != QueueState::Processing {
            return Err(WorkerQueueError::InvalidState {
                request_id,
                expected: QueueState::Processing,
                actual: state,
            });
        }
        let processing_path = self.state_path(QueueState::Processing, request_id);
        validate_claim_token(&processing_path, token, QueueState::Processing)?;
        match error_code {
            Some(error_code) => ensure_result_record(
                self,
                &processing_path,
                token,
                error_code,
                QueueState::Processing,
            )?,
            None => ensure_result_absent(&processing_path, request_id, QueueState::Processing)?,
        }
        let terminal_path = self.state_path(terminal_state, request_id);
        self.current_identity_locked()
            .map_err(WorkerQueueError::Queue)?;
        fs::rename(&processing_path, terminal_path).map_err(WorkerQueueError::Io)?;
        sync_directory(self.state_root(terminal_state)).map_err(WorkerQueueError::Queue)?;
        sync_directory(self.state_root(QueueState::Processing)).map_err(WorkerQueueError::Queue)?;
        self.current_identity_locked()
            .map(|_| ())
            .map_err(WorkerQueueError::Queue)
    }

    pub(super) fn fail_pending(
        &self,
        request_id: RequestId,
        batch_id: BatchId,
        error_code: ErrorCode,
    ) -> Result<(), WorkerQueueError> {
        let queue_lock = self.open_queue_lock().map_err(WorkerQueueError::Queue)?;
        queue_lock.lock().map_err(WorkerQueueError::Io)?;
        self.current_identity_locked()
            .map_err(WorkerQueueError::Queue)?;

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
        write_result_record(self, &pending_path, &record)?;

        let failed_path = self.state_path(QueueState::Failed, request_id);
        self.current_identity_locked()
            .map_err(WorkerQueueError::Queue)?;
        fs::rename(&pending_path, failed_path).map_err(WorkerQueueError::Io)?;
        sync_directory(self.state_root(QueueState::Failed)).map_err(WorkerQueueError::Queue)?;
        sync_directory(self.state_root(QueueState::Pending)).map_err(WorkerQueueError::Queue)?;
        self.current_identity_locked()
            .map_err(WorkerQueueError::Queue)?;
        Ok(())
    }
}

fn sort_outcomes(outcomes: &mut [ReconciledOutcome]) {
    outcomes.sort_unstable_by_key(|outcome| match outcome {
        ReconciledOutcome::Completed(token) | ReconciledOutcome::Failed(token, _) => {
            token.request_id()
        }
    });
}

fn validate_terminal_claim(
    queue: &FileQueue,
    token: ClaimToken,
    state: QueueState,
    error_code: Option<ErrorCode>,
) -> Result<(), WorkerQueueError> {
    let request_id = token.request_id();
    let root = queue.state_path(state, request_id);
    validate_claim_token(&root, token, state)?;
    match error_code {
        Some(error_code) => validate_result_record(&root, token, error_code, state),
        None => ensure_result_absent(&root, request_id, state),
    }
}

fn validate_claim_token(
    package_root: &std::path::Path,
    token: ClaimToken,
    state: QueueState,
) -> Result<(), WorkerQueueError> {
    let request_id = token.request_id();
    let record = read_required_phase_record(package_root, request_id, state)?;
    if record.batch_id != token.batch_id()
        || record.attempt != token.attempt()
        || record.phase != WorkerPhase::Claimed
    {
        return Err(WorkerQueueError::ClaimChanged { request_id });
    }
    Ok(())
}

fn ensure_result_absent(
    package_root: &std::path::Path,
    request_id: RequestId,
    state: QueueState,
) -> Result<(), WorkerQueueError> {
    match fs::symlink_metadata(package_root.join(RESULT_FILE_NAME)) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Ok(_) => Err(WorkerQueueError::CorruptState {
            request_id,
            state,
            detail: "successful request contains failure result metadata",
        }),
        Err(error) => Err(WorkerQueueError::Io(error)),
    }
}

fn ensure_result_record(
    queue: &FileQueue,
    package_root: &std::path::Path,
    token: ClaimToken,
    error_code: ErrorCode,
    state: QueueState,
) -> Result<(), WorkerQueueError> {
    match fs::symlink_metadata(package_root.join(RESULT_FILE_NAME)) {
        Ok(_) => validate_result_record(package_root, token, error_code, state),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let record = WorkerResultRecord {
                schema_version: CURRENT_WORKER_RESULT_SCHEMA_VERSION,
                request_id: token.request_id(),
                batch_id: token.batch_id(),
                status: WorkerResultStatus::Failed,
                error_code,
                failed_at: OffsetDateTime::now_utc(),
            };
            write_result_record(queue, package_root, &record)
        }
        Err(error) => Err(WorkerQueueError::Io(error)),
    }
}

fn validate_result_record(
    package_root: &std::path::Path,
    token: ClaimToken,
    error_code: ErrorCode,
    state: QueueState,
) -> Result<(), WorkerQueueError> {
    let path = package_root.join(RESULT_FILE_NAME);
    let metadata = fs::symlink_metadata(&path).map_err(WorkerQueueError::Io)?;
    if !metadata.file_type().is_file() || metadata.len() > MAXIMUM_RESULT_FILE_BYTES {
        return Err(WorkerQueueError::CorruptState {
            request_id: token.request_id(),
            state,
            detail: "result metadata is not a bounded regular file",
        });
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)
        .map_err(WorkerQueueError::Io)?
        .take(MAXIMUM_RESULT_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(WorkerQueueError::Io)?;
    let record: WorkerResultRecord =
        serde_json::from_slice(&bytes).map_err(WorkerQueueError::InvalidResultMetadata)?;
    if record.schema_version != CURRENT_WORKER_RESULT_SCHEMA_VERSION
        || record.request_id != token.request_id()
        || record.batch_id != token.batch_id()
        || record.status != WorkerResultStatus::Failed
        || record.error_code != error_code
    {
        return Err(WorkerQueueError::CorruptState {
            request_id: token.request_id(),
            state,
            detail: "result metadata does not match the terminal outcome",
        });
    }
    Ok(())
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
    record: &WorkerResultRecord,
) -> Result<(), WorkerQueueError> {
    let temporary_root = queue.worker_temporary_root();
    let temporary_path = temporary_root.join(format!(".result-{}", Ulid::generate()));
    let destination = package_root.join(RESULT_FILE_NAME);
    match fs::symlink_metadata(&destination) {
        Ok(metadata) if metadata.file_type().is_file() => {}
        Ok(_) => {
            return Err(WorkerQueueError::CorruptState {
                request_id: record.request_id,
                state: QueueState::Pending,
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
        sync_directory(temporary_root).map_err(WorkerQueueError::Queue)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary_path);
    }
    result
}

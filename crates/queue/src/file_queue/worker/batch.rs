use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::path::Path;

use agent_knowledge_core::{BatchId, RequestId, Revision};

use super::{ClaimedPackage, WorkerQueueError, next_attempt, revalidate_accepted};
use crate::file_queue::{FileQueue, NEXT_SEQUENCE_FILE_NAME, QueueState, read_next_sequence};

/// Progress made while selecting and claiming one bounded Worker batch.
#[derive(Debug)]
pub enum BatchClaimOutcome {
    /// The bounded scan has more pending entries to inspect.
    Scanning {
        /// Directory entries inspected by this invocation.
        scanned_entries: usize,
        /// Earliest eligible requests retained for the batch.
        retained_candidates: usize,
    },
    /// The complete snapshot was scanned and its earliest requests were claimed.
    Claimed(Vec<ClaimedPackage>),
}

/// Exclusive Repository Worker access to one file queue.
#[derive(Debug)]
pub struct WorkerSession {
    pub(super) queue: FileQueue,
    _writer_lock: File,
    pending_scan: PendingBatchScan,
    pub(super) processing_scan: Option<fs::ReadDir>,
    pub(super) processing_recovery_complete: bool,
}

#[derive(Debug, Default)]
struct PendingBatchScan {
    entries: Option<fs::ReadDir>,
    candidates: BinaryHeap<PendingCandidate>,
    batch_id: Option<BatchId>,
    maximum_requests: usize,
    maximum_sequence: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingCandidate {
    sequence: u64,
    request_id: RequestId,
}

impl Ord for PendingCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.sequence
            .cmp(&other.sequence)
            .then_with(|| self.request_id.cmp(&other.request_id))
    }
}

impl PartialOrd for PendingCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl FileQueue {
    /// Acquires exclusive Repository Worker ownership for this queue.
    ///
    /// The writer lock is distinct from the short-lived queue lock used by the
    /// Gateway, so accepted requests can continue arriving while one Worker
    /// owns processing transitions.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerQueueError::WorkerAlreadyRunning`] without waiting when
    /// another process owns the writer lock, or an I/O error when the lock path
    /// cannot be initialized.
    pub fn try_worker_session(&self) -> Result<WorkerSession, WorkerQueueError> {
        let writer_lock = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.stable_worker_lock_file)
            .map_err(WorkerQueueError::Io)?;
        match writer_lock.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(WorkerQueueError::WorkerAlreadyRunning);
            }
            Err(TryLockError::Error(error)) => return Err(WorkerQueueError::Io(error)),
        }
        self.current_identity_locked()
            .map_err(WorkerQueueError::Queue)?;
        Ok(WorkerSession {
            queue: self.clone(),
            _writer_lock: writer_lock,
            pending_scan: PendingBatchScan::default(),
            processing_scan: None,
            processing_recovery_complete: false,
        })
    }
}

impl WorkerSession {
    /// Returns a stable digest identifying this queue's canonical root.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the queue root can no longer be canonicalized.
    pub fn queue_identity(&self) -> Result<Revision, WorkerQueueError> {
        let queue_lock = self
            .queue
            .open_queue_lock()
            .map_err(WorkerQueueError::Queue)?;
        queue_lock.lock().map_err(WorkerQueueError::Io)?;
        self.queue
            .current_identity_locked()
            .map_err(WorkerQueueError::Queue)
    }

    /// Verifies that this live Worker can perform one repository transaction.
    ///
    /// # Errors
    ///
    /// Returns an error until processing recovery is complete or while a
    /// pending or processing scan is active.
    pub fn ensure_transaction_ready(&self) -> Result<(), WorkerQueueError> {
        self.queue_identity()?;
        self.ensure_no_active_scan()
    }

    /// Revalidates that one claim is still owned by this live Worker session.
    ///
    /// # Errors
    ///
    /// Returns an error when recovery is incomplete, another scan is active,
    /// or the processing package no longer matches its exact claim token.
    pub fn validate_claimed(
        &mut self,
        claim: &super::ClaimedPackage,
    ) -> Result<(), WorkerQueueError> {
        self.queue_identity()?;
        self.ensure_no_active_scan()?;
        self.queue.validate_claimed(claim)
    }

    /// Claims one known pending request while holding Worker ownership.
    ///
    /// # Errors
    ///
    /// Returns an error when the request cannot be durably claimed.
    pub fn claim(
        &mut self,
        request_id: RequestId,
        batch_id: BatchId,
    ) -> Result<ClaimedPackage, WorkerQueueError> {
        self.queue_identity()?;
        self.ensure_no_active_scan()?;
        let result = self.queue.claim(request_id, batch_id);
        if result.is_err() {
            self.require_processing_recovery();
        }
        result
    }

    /// Returns one still-owned processing request to `pending/`.
    ///
    /// # Errors
    ///
    /// Returns an error when the token is stale or the request cannot be
    /// durably moved.
    pub fn requeue_claimed(&mut self, token: super::ClaimToken) -> Result<(), WorkerQueueError> {
        self.queue_identity()?;
        self.ensure_no_active_scan()?;
        let result = self.queue.requeue_claimed(token);
        if result.is_err() {
            self.require_processing_recovery();
        }
        result
    }

    /// Incrementally scans a fixed pending snapshot and claims its earliest requests.
    ///
    /// Each invocation inspects at most `maximum_scan_entries` directory
    /// entries and retains at most `maximum_requests` candidates. Call again
    /// with the same batch ID and request limit while [`BatchClaimOutcome::Scanning`]
    /// is returned. New requests accepted after the first call are left for a
    /// later batch.
    ///
    /// # Errors
    ///
    /// Returns an error for zero limits, changed scan parameters, malformed
    /// queue entries, transient package reads, or failed durable claims. If
    /// final claiming fails, callers must use
    /// [`WorkerSession::scan_processing`] to recover any claims whose state
    /// transition completed before the error.
    pub fn claim_next_batch(
        &mut self,
        batch_id: BatchId,
        maximum_scan_entries: usize,
        maximum_requests: usize,
    ) -> Result<BatchClaimOutcome, WorkerQueueError> {
        self.queue_identity()?;
        if maximum_scan_entries == 0 || maximum_requests == 0 {
            return Err(WorkerQueueError::InvalidBatchLimits);
        }
        self.ensure_recovery_complete()?;
        if self.processing_scan.is_some() {
            return Err(WorkerQueueError::ProcessingScanInProgress);
        }
        if let Some(active_batch_id) = self.pending_scan.batch_id
            && (active_batch_id != batch_id
                || self.pending_scan.maximum_requests != maximum_requests)
        {
            return Err(WorkerQueueError::BatchScanChanged { active_batch_id });
        }

        if self.pending_scan.entries.is_none() {
            let queue_lock = self
                .queue
                .open_queue_lock()
                .map_err(WorkerQueueError::Queue)?;
            queue_lock.lock().map_err(WorkerQueueError::Io)?;
            self.queue
                .current_identity_locked()
                .map_err(WorkerQueueError::Queue)?;
            let next_sequence =
                read_next_sequence(&self.queue.queue_root.join(NEXT_SEQUENCE_FILE_NAME))
                    .map_err(WorkerQueueError::Queue)?;
            self.pending_scan.entries = Some(
                fs::read_dir(
                    self.queue
                        .queue_root
                        .join(QueueState::Pending.directory_name()),
                )
                .map_err(WorkerQueueError::Io)?,
            );
            self.pending_scan.batch_id = Some(batch_id);
            self.pending_scan.maximum_requests = maximum_requests;
            self.pending_scan.maximum_sequence = next_sequence - 1;
        }

        let mut scanned_entries = 0_usize;
        let mut complete = false;
        while scanned_entries < maximum_scan_entries {
            let entry = match self.pending_scan.entries.as_mut() {
                Some(entries) => entries.next(),
                None => None,
            };
            let Some(entry) = entry else {
                complete = true;
                break;
            };
            scanned_entries += 1;
            let result = entry.map_err(WorkerQueueError::Io).and_then(|entry| {
                retain_pending_candidate(
                    &self.queue,
                    &mut self.pending_scan,
                    entry,
                    batch_id,
                    maximum_requests,
                )
            });
            if let Err(error) = result {
                self.pending_scan = PendingBatchScan::default();
                return Err(error);
            }
        }

        if !complete {
            return Ok(BatchClaimOutcome::Scanning {
                scanned_entries,
                retained_candidates: self.pending_scan.candidates.len(),
            });
        }

        let candidates = std::mem::take(&mut self.pending_scan.candidates).into_sorted_vec();
        self.pending_scan = PendingBatchScan::default();
        let mut prepared = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let claim = self.queue.prepare_claim(candidate.request_id)?;
            prepared.push((candidate.request_id, claim));
        }

        let mut claimed = Vec::with_capacity(prepared.len());
        for (request_id, prepared) in prepared {
            let claim = self.queue.claim_prepared(request_id, batch_id, prepared);
            match claim {
                Ok(claim) => claimed.push(claim),
                Err(error) => {
                    self.require_processing_recovery();
                    return Err(error);
                }
            }
        }
        Ok(BatchClaimOutcome::Claimed(claimed))
    }

    pub(super) fn ensure_no_active_scan(&self) -> Result<(), WorkerQueueError> {
        self.ensure_recovery_complete()?;
        match self.pending_scan.batch_id {
            Some(active_batch_id) => Err(WorkerQueueError::BatchScanInProgress { active_batch_id }),
            None if self.processing_scan.is_some() => {
                Err(WorkerQueueError::ProcessingScanInProgress)
            }
            None => Ok(()),
        }
    }

    pub(super) const fn active_batch_id(&self) -> Option<BatchId> {
        self.pending_scan.batch_id
    }

    pub(super) const fn ensure_recovery_complete(&self) -> Result<(), WorkerQueueError> {
        if self.processing_recovery_complete {
            Ok(())
        } else if self.processing_scan.is_some() {
            Err(WorkerQueueError::ProcessingScanInProgress)
        } else {
            Err(WorkerQueueError::ProcessingRecoveryRequired)
        }
    }

    pub(super) fn require_processing_recovery(&mut self) {
        self.processing_recovery_complete = false;
        self.processing_scan = None;
    }
}

fn retain_pending_candidate(
    queue: &FileQueue,
    scan: &mut PendingBatchScan,
    entry: fs::DirEntry,
    batch_id: BatchId,
    maximum_requests: usize,
) -> Result<(), WorkerQueueError> {
    let path = entry.path();
    if !entry.file_type().map_err(WorkerQueueError::Io)?.is_dir() {
        return Err(WorkerQueueError::InvalidPendingEntry {
            path,
            detail: "entry is not a directory",
        });
    }
    let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
        return Err(WorkerQueueError::InvalidPendingEntry {
            path,
            detail: "entry name is not UTF-8",
        });
    };
    let request_id: RequestId =
        name.parse()
            .map_err(|_| WorkerQueueError::InvalidPendingEntry {
                path: path.clone(),
                detail: "entry name is not a canonical request ID",
            })?;
    if request_id.to_string() != name {
        return Err(WorkerQueueError::InvalidPendingEntry {
            path,
            detail: "entry name is not a canonical request ID",
        });
    }
    let candidate = inspect_pending_candidate(queue, scan, &path, request_id);
    let candidate = match candidate {
        Ok(candidate) => candidate,
        Err(error)
            if matches!(
                &error,
                WorkerQueueError::CorruptPackage { .. }
                    | WorkerQueueError::CorruptState { .. }
                    | WorkerQueueError::InvalidPhaseMetadata(_)
            ) =>
        {
            queue.fail_pending(request_id, batch_id, error.error_code())?;
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    let Some(candidate) = candidate else {
        return Ok(());
    };
    scan.candidates.push(candidate);
    if scan.candidates.len() > maximum_requests {
        let _ = scan.candidates.pop();
    }
    Ok(())
}

fn inspect_pending_candidate(
    queue: &FileQueue,
    scan: &PendingBatchScan,
    path: &Path,
    request_id: RequestId,
) -> Result<Option<PendingCandidate>, WorkerQueueError> {
    let package = revalidate_accepted(path, request_id, QueueState::Pending, &queue.policy)?;
    if package.request().request_id != request_id {
        return Err(WorkerQueueError::CorruptState {
            request_id,
            state: QueueState::Pending,
            detail: "accepted package identity does not match its queue entry",
        });
    }
    let acceptance = package.acceptance().ok_or(WorkerQueueError::CorruptState {
        request_id,
        state: QueueState::Pending,
        detail: "accepted package is missing acceptance metadata",
    })?;
    let sequence = acceptance.sequence.get();
    if sequence > scan.maximum_sequence {
        return Ok(None);
    }
    let _ = next_attempt(path, request_id, QueueState::Pending)?;
    Ok(Some(PendingCandidate {
        sequence,
        request_id,
    }))
}

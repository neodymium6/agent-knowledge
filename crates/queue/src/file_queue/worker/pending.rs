use std::fs;
use std::path::PathBuf;

use agent_knowledge_core::RequestId;
use time::OffsetDateTime;

use super::{WorkerQueueError, WorkerSession};
use crate::PackageValidationError;
use crate::file_queue::{NEXT_SEQUENCE_FILE_NAME, QueueState, read_next_sequence};
use crate::package::read_acceptance_file;

const ACCEPTANCE_FILE_NAME: &str = "acceptance.json";

/// Bounded progress while observing one fixed pending acceptance snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PendingScanOutcome {
    /// More directory entries remain in the current observation.
    Scanning {
        /// Directory entries inspected by this invocation.
        scanned_entries: usize,
        /// Requests observed in the fixed snapshot so far.
        observed_requests: usize,
    },
    /// The complete fixed snapshot has been observed.
    Complete(PendingSnapshot),
}

/// Aggregate pending state used to decide when a Worker batch closes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PendingSnapshot {
    maximum_sequence: u64,
    requests: usize,
    oldest_accepted_at: Option<OffsetDateTime>,
    newest_accepted_at: Option<OffsetDateTime>,
    has_invalid_acceptance: bool,
}

impl PendingSnapshot {
    /// Returns the fixed upper acceptance-sequence boundary for this observation.
    #[must_use]
    pub const fn maximum_sequence(self) -> u64 {
        self.maximum_sequence
    }

    /// Returns the number of pending entries included in this observation.
    #[must_use]
    pub const fn requests(self) -> usize {
        self.requests
    }

    /// Returns the oldest valid acceptance timestamp in this observation.
    #[must_use]
    pub const fn oldest_accepted_at(self) -> Option<OffsetDateTime> {
        self.oldest_accepted_at
    }

    /// Returns the newest valid acceptance timestamp in this observation.
    #[must_use]
    pub const fn newest_accepted_at(self) -> Option<OffsetDateTime> {
        self.newest_accepted_at
    }

    /// Returns whether an entry had invalid immutable acceptance metadata.
    #[must_use]
    pub const fn has_invalid_acceptance(self) -> bool {
        self.has_invalid_acceptance
    }

    /// Returns whether no pending entries were present in the fixed snapshot.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.requests == 0
    }
}

#[derive(Debug, Default)]
pub(in crate::file_queue::worker) struct PendingObservationScan {
    entries: Option<fs::ReadDir>,
    maximum_sequence: u64,
    requests: usize,
    oldest_accepted_at: Option<OffsetDateTime>,
    newest_accepted_at: Option<OffsetDateTime>,
    has_invalid_acceptance: bool,
}

impl PendingObservationScan {
    pub(super) const fn is_active(&self) -> bool {
        self.entries.is_some()
    }

    fn snapshot(&self) -> PendingSnapshot {
        PendingSnapshot {
            maximum_sequence: self.maximum_sequence,
            requests: self.requests,
            oldest_accepted_at: self.oldest_accepted_at,
            newest_accepted_at: self.newest_accepted_at,
            has_invalid_acceptance: self.has_invalid_acceptance,
        }
    }
}

impl WorkerSession {
    /// Incrementally observes a fixed pending acceptance snapshot.
    ///
    /// Each invocation inspects at most `maximum_scan_entries`. Requests
    /// accepted after the first invocation are excluded, allowing the returned
    /// sequence boundary to be claimed without absorbing later arrivals.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero limit, incomplete processing recovery,
    /// another active scan, structurally invalid pending entries, or transient
    /// file-system failures.
    pub fn scan_pending(
        &mut self,
        maximum_scan_entries: usize,
    ) -> Result<PendingScanOutcome, WorkerQueueError> {
        self.queue_identity()?;
        if maximum_scan_entries == 0 {
            return Err(WorkerQueueError::InvalidBatchLimits);
        }
        self.ensure_recovery_complete()?;
        if let Some(active_batch_id) = self.active_batch_id() {
            return Err(WorkerQueueError::BatchScanInProgress { active_batch_id });
        }
        if self.processing_scan.is_some() {
            return Err(WorkerQueueError::ProcessingScanInProgress);
        }
        if !self.pending_observation.is_active() {
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
            self.pending_observation.maximum_sequence = next_sequence - 1;
            self.pending_observation.entries = Some(
                fs::read_dir(self.queue.state_root(QueueState::Pending))
                    .map_err(WorkerQueueError::Io)?,
            );
        }

        let mut scanned_entries = 0;
        let mut complete = false;
        while scanned_entries < maximum_scan_entries {
            let entry = match self.pending_observation.entries.as_mut() {
                Some(entries) => entries.next(),
                None => None,
            };
            let Some(entry) = entry else {
                complete = true;
                break;
            };
            scanned_entries += 1;
            let result = entry
                .map_err(WorkerQueueError::Io)
                .and_then(|entry| inspect_pending(&mut self.pending_observation, entry));
            if let Err(error) = result {
                self.pending_observation = PendingObservationScan::default();
                return Err(error);
            }
        }

        if complete {
            let snapshot = self.pending_observation.snapshot();
            self.pending_observation = PendingObservationScan::default();
            Ok(PendingScanOutcome::Complete(snapshot))
        } else {
            Ok(PendingScanOutcome::Scanning {
                scanned_entries,
                observed_requests: self.pending_observation.requests,
            })
        }
    }
}

fn inspect_pending(
    scan: &mut PendingObservationScan,
    entry: fs::DirEntry,
) -> Result<(), WorkerQueueError> {
    let (path, _request_id) = pending_request_entry(entry)?;
    let acceptance = match read_acceptance_file(&path.join(ACCEPTANCE_FILE_NAME)) {
        Ok(acceptance) => acceptance,
        Err(PackageValidationError::Io(error)) => return Err(WorkerQueueError::Io(error)),
        Err(_) => {
            scan.requests = scan.requests.saturating_add(1);
            scan.has_invalid_acceptance = true;
            return Ok(());
        }
    };
    if acceptance.sequence.get() > scan.maximum_sequence {
        return Ok(());
    }
    scan.requests = scan.requests.saturating_add(1);
    scan.oldest_accepted_at = Some(
        scan.oldest_accepted_at
            .map_or(acceptance.accepted_at, |oldest| {
                oldest.min(acceptance.accepted_at)
            }),
    );
    scan.newest_accepted_at = Some(
        scan.newest_accepted_at
            .map_or(acceptance.accepted_at, |newest| {
                newest.max(acceptance.accepted_at)
            }),
    );
    Ok(())
}

fn pending_request_entry(entry: fs::DirEntry) -> Result<(PathBuf, RequestId), WorkerQueueError> {
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
    let request_id =
        name.parse::<RequestId>()
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
    Ok((path, request_id))
}

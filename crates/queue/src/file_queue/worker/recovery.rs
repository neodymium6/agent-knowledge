use std::fs;

use agent_knowledge_core::RequestId;

use super::{
    ClaimToken, ClaimedPackage, WorkerPhase, WorkerQueueError, WorkerSession,
    read_required_phase_record, revalidate_accepted,
};
use crate::file_queue::QueueState;

/// Bounded progress while discovering durable interrupted claims.
#[derive(Debug)]
pub enum ProcessingScanOutcome {
    /// More `processing/` entries remain to be inspected.
    Scanning {
        /// Directory entries inspected by this invocation.
        scanned_entries: usize,
        /// Valid durable claims discovered by this invocation.
        claims: Vec<ClaimedPackage>,
    },
    /// The complete `processing/` directory has been inspected.
    Complete {
        /// Directory entries inspected by this invocation.
        scanned_entries: usize,
        /// Valid durable claims discovered by this invocation.
        claims: Vec<ClaimedPackage>,
    },
}

impl WorkerSession {
    /// Incrementally discovers claims left in `processing/` by an interrupted Worker.
    ///
    /// Callers retain each returned claim and continue until
    /// [`ProcessingScanOutcome::Complete`]. No other Worker transition is
    /// allowed until the scan completes, after which each exact token can be
    /// resumed or requeued.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero scan limit, an active pending-batch scan,
    /// malformed processing entries, corrupt packages or phase records, and
    /// file-system failures.
    pub fn scan_processing(
        &mut self,
        maximum_scan_entries: usize,
    ) -> Result<ProcessingScanOutcome, WorkerQueueError> {
        if maximum_scan_entries == 0 {
            return Err(WorkerQueueError::InvalidProcessingScanLimit);
        }
        if let Some(active_batch_id) = self.active_batch_id() {
            return Err(WorkerQueueError::BatchScanInProgress { active_batch_id });
        }
        if self.processing_scan.is_none() {
            self.processing_recovery_complete = false;
            self.processing_scan = Some(
                fs::read_dir(
                    self.queue
                        .queue_root
                        .join(QueueState::Processing.directory_name()),
                )
                .map_err(WorkerQueueError::Io)?,
            );
        }

        let mut scanned_entries = 0_usize;
        let mut claims = Vec::new();
        let mut complete = false;
        while scanned_entries < maximum_scan_entries {
            let entry = match self.processing_scan.as_mut() {
                Some(entries) => entries.next(),
                None => None,
            };
            let Some(entry) = entry else {
                complete = true;
                break;
            };
            scanned_entries += 1;
            let claim = entry
                .map_err(WorkerQueueError::Io)
                .and_then(|entry| recover_processing_claim(&self.queue, entry));
            match claim {
                Ok(claim) => claims.push(claim),
                Err(error) => {
                    self.processing_scan = None;
                    self.processing_recovery_complete = false;
                    return Err(error);
                }
            }
        }

        if complete {
            self.processing_scan = None;
            self.processing_recovery_complete = true;
            Ok(ProcessingScanOutcome::Complete {
                scanned_entries,
                claims,
            })
        } else {
            Ok(ProcessingScanOutcome::Scanning {
                scanned_entries,
                claims,
            })
        }
    }
}

fn recover_processing_claim(
    queue: &crate::file_queue::FileQueue,
    entry: fs::DirEntry,
) -> Result<ClaimedPackage, WorkerQueueError> {
    let path = entry.path();
    if !entry.file_type().map_err(WorkerQueueError::Io)?.is_dir() {
        return Err(WorkerQueueError::InvalidProcessingEntry {
            path,
            detail: "entry is not a directory",
        });
    }
    let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
        return Err(WorkerQueueError::InvalidProcessingEntry {
            path,
            detail: "entry name is not UTF-8",
        });
    };
    let request_id: RequestId =
        name.parse()
            .map_err(|_| WorkerQueueError::InvalidProcessingEntry {
                path: path.clone(),
                detail: "entry name is not a canonical request ID",
            })?;
    if request_id.to_string() != name {
        return Err(WorkerQueueError::InvalidProcessingEntry {
            path,
            detail: "entry name is not a canonical request ID",
        });
    }

    let package = revalidate_accepted(&path, request_id, QueueState::Processing, &queue.policy)?;
    if package.request().request_id != request_id {
        return Err(WorkerQueueError::CorruptState {
            request_id,
            state: QueueState::Processing,
            detail: "accepted package identity does not match its queue entry",
        });
    }
    let record = read_required_phase_record(&path, request_id, QueueState::Processing)?;
    if record.phase != WorkerPhase::Claimed {
        return Err(WorkerQueueError::CorruptState {
            request_id,
            state: QueueState::Processing,
            detail: "processing package has an unsupported recovery phase",
        });
    }
    Ok(ClaimedPackage {
        token: ClaimToken {
            request_id,
            batch_id: record.batch_id,
            attempt: record.attempt,
        },
        package,
    })
}

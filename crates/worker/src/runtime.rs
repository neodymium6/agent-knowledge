use std::fmt;
use std::io;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration as StandardDuration;

use agent_knowledge_core::BatchId;
use agent_knowledge_queue::{
    BatchClaimOutcome, ClaimedPackage, FileQueue, PendingScanOutcome, ProcessingScanOutcome,
    WorkerQueueError, WorkerSession,
};
use agent_knowledge_repository::{
    BatchCommitOutcome, RemoteReplicationError, RemoteReplicationOutcome, RemoteReplicator,
    RepositoryTransaction,
};
use time::OffsetDateTime;

use crate::{BatchCloseReason, BatchProcessor, BatchProcessorError, BatchReadiness, BatchSchedule};

const REPLICATION_VERIFICATION_INTERVAL: StandardDuration = StandardDuration::from_secs(30);
const REPLICATION_EVENT_RETRY_INTERVAL: StandardDuration = StandardDuration::from_millis(100);
const REPLICATION_EVENT_CAPACITY: usize = 64;

type ReplicationEvent = Result<RemoteReplicationOutcome, RemoteReplicationError>;

/// Bounded queue scan and batch-size limits for one Worker process.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerRunLimits {
    maximum_scan_entries: NonZeroUsize,
    maximum_requests: NonZeroUsize,
    maximum_recovery_requests: NonZeroUsize,
}

impl WorkerRunLimits {
    /// Creates limits for incremental directory scans and in-memory claims.
    ///
    /// A recovery bound below the new-batch bound is raised to the new-batch
    /// bound so every batch created by this configuration remains recoverable.
    #[must_use]
    pub const fn new(
        maximum_scan_entries: NonZeroUsize,
        maximum_requests: NonZeroUsize,
        maximum_recovery_requests: NonZeroUsize,
    ) -> Self {
        let maximum_recovery_requests = if maximum_recovery_requests.get() < maximum_requests.get()
        {
            maximum_requests
        } else {
            maximum_recovery_requests
        };
        Self {
            maximum_scan_entries,
            maximum_requests,
            maximum_recovery_requests,
        }
    }

    /// Returns the number of directory entries inspected per scan step.
    #[must_use]
    pub const fn maximum_scan_entries(self) -> NonZeroUsize {
        self.maximum_scan_entries
    }

    /// Returns the maximum claims retained and processed in one batch.
    #[must_use]
    pub const fn maximum_requests(self) -> NonZeroUsize {
        self.maximum_requests
    }

    /// Returns the stable safety bound for claims retained during startup.
    ///
    /// This bound is independent of the size selected for new batches, so a
    /// deployment can reduce new batch sizes without stranding older work.
    #[must_use]
    pub const fn maximum_recovery_requests(self) -> NonZeroUsize {
        self.maximum_recovery_requests
    }
}

impl Default for WorkerRunLimits {
    fn default() -> Self {
        Self::new(
            NonZeroUsize::new(1_024).unwrap_or(NonZeroUsize::MIN),
            NonZeroUsize::new(100).unwrap_or(NonZeroUsize::MIN),
            NonZeroUsize::new(10_000).unwrap_or(NonZeroUsize::MIN),
        )
    }
}

/// Durable work completed before a Worker accepts a new batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StartupOutcome {
    /// No interrupted queue claims or repository journal existed.
    Clean,
    /// Claims made before a repository transaction began were returned to pending.
    Requeued {
        /// Batch whose claims were returned.
        batch_id: BatchId,
        /// Number of requests returned to pending.
        requests: usize,
    },
    /// An interrupted repository transaction was completed.
    Resumed {
        /// Batch whose transaction was completed.
        batch_id: BatchId,
        /// Exact committed or no-change result.
        outcome: BatchCommitOutcome,
        /// Requests rejected before the recovered transaction began.
        claim_failures: usize,
    },
}

/// Result of interruptible Worker startup.
#[derive(Debug)]
pub enum InterruptibleStart<T> {
    /// Startup recovery completed and produced a ready value.
    Started(T),
    /// Shutdown was observed before startup recovery could complete.
    Stopped {
        /// Requests rejected before shutdown was observed.
        failed_requests: usize,
    },
}

impl<T> InterruptibleStart<T> {
    /// Maps the ready startup value without changing a stopped result.
    pub fn map<U>(self, map: impl FnOnce(T) -> U) -> InterruptibleStart<U> {
        match self {
            Self::Started(value) => InterruptibleStart::Started(map(value)),
            Self::Stopped { failed_requests } => InterruptibleStart::Stopped { failed_requests },
        }
    }
}

/// Result of selecting and processing at most one new batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkerRunOutcome {
    /// The fixed pending snapshot contained no requests.
    Idle,
    /// The fixed snapshot contained only requests rejected during claiming.
    ClosedWithoutProcessing {
        /// Requests rejected while the snapshot was claimed.
        failed_requests: usize,
    },
    /// One newly claimed batch reached its terminal outcome.
    Processed {
        /// Newly generated batch identifier.
        batch_id: BatchId,
        /// Exact committed or no-change result.
        outcome: BatchCommitOutcome,
        /// Requests rejected before repository processing began.
        claim_failures: usize,
    },
}

/// Progress made by one nonblocking scheduler poll.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkerPollOutcome {
    /// Shutdown was requested before a repository transaction began.
    ///
    /// This is terminal for the runtime; the caller must discard it and use
    /// startup recovery before processing more work.
    Stopped {
        /// Requests rejected before shutdown was observed.
        failed_requests: usize,
    },
    /// One bounded portion of the pending directory was observed.
    Scanning {
        /// Directory entries inspected by this poll.
        scanned_entries: usize,
        /// Requests observed in the fixed snapshot so far.
        observed_requests: usize,
    },
    /// The complete fixed snapshot contained no requests.
    Idle,
    /// The snapshot remains open until a time threshold is reached.
    Waiting {
        /// Earliest time at which an unchanged snapshot becomes ready.
        ready_at: OffsetDateTime,
    },
    /// A ready snapshot contained only entries failed during claim validation.
    ClosedWithoutCommit {
        /// Threshold responsible for closing the snapshot.
        reason: BatchCloseReason,
        /// Requests rejected while the snapshot was claimed.
        failed_requests: usize,
    },
    /// A ready snapshot was claimed and processed.
    Processed {
        /// Threshold responsible for closing the batch.
        reason: BatchCloseReason,
        /// Newly generated batch identifier.
        batch_id: BatchId,
        /// Exact committed or no-change result.
        outcome: BatchCommitOutcome,
        /// Requests rejected before repository processing began.
        claim_failures: usize,
    },
}

#[derive(Debug)]
struct ReplicationBackground {
    control: Arc<ReplicationControl>,
    events: Receiver<ReplicationEvent>,
    thread: Option<JoinHandle<()>>,
}

#[derive(Debug)]
struct ReplicationControl {
    stopping: AtomicBool,
    generation: Mutex<u64>,
    wake: Condvar,
}

impl ReplicationBackground {
    fn start(replication: RemoteReplicator) -> Result<Self, io::Error> {
        let control = Arc::new(ReplicationControl {
            stopping: AtomicBool::new(false),
            generation: Mutex::new(0),
            wake: Condvar::new(),
        });
        let (event_sender, events) = sync_channel(REPLICATION_EVENT_CAPACITY);
        let thread_control = Arc::clone(&control);
        let thread = thread::Builder::new()
            .name("knowledge-git-replication".into())
            .spawn(move || replication_loop(replication, &thread_control, &event_sender))?;
        Ok(Self {
            control,
            events,
            thread: Some(thread),
        })
    }

    fn wake(&self) {
        self.control.wake();
    }

    fn take_event(&self) -> Option<ReplicationEvent> {
        self.events.try_recv().ok()
    }
}

impl Drop for ReplicationBackground {
    fn drop(&mut self) {
        self.control.stop();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl ReplicationControl {
    fn generation(&self) -> u64 {
        match self.generation.lock() {
            Ok(generation) => *generation,
            Err(poisoned) => *poisoned.into_inner(),
        }
    }

    fn wake(&self) {
        let mut generation = match self.generation.lock() {
            Ok(generation) => generation,
            Err(poisoned) => poisoned.into_inner(),
        };
        *generation = generation.wrapping_add(1);
        self.wake.notify_all();
    }

    fn stop(&self) {
        let generation = match self.generation.lock() {
            Ok(generation) => generation,
            Err(poisoned) => poisoned.into_inner(),
        };
        self.stopping.store(true, Ordering::Release);
        self.wake.notify_all();
        drop(generation);
    }

    fn wait_after(&self, observed_generation: u64, timeout: StandardDuration) {
        let generation = match self.generation.lock() {
            Ok(generation) => generation,
            Err(poisoned) => poisoned.into_inner(),
        };
        if self.stopping.load(Ordering::Acquire) || *generation != observed_generation {
            return;
        }
        drop(
            self.wake
                .wait_timeout_while(generation, timeout, |generation| {
                    !self.stopping.load(Ordering::Acquire) && *generation == observed_generation
                }),
        );
    }
}

fn replication_loop(
    replication: RemoteReplicator,
    control: &ReplicationControl,
    events: &SyncSender<ReplicationEvent>,
) {
    let mut error_reported = false;
    let mut pending_event = None;
    while !control.stopping.load(Ordering::Acquire) {
        if let Some(event) = pending_event.take() {
            match events.try_send(event) {
                Ok(()) => {}
                Err(TrySendError::Full(event)) => {
                    pending_event = Some(event);
                    let generation = control.generation();
                    control.wait_after(generation, REPLICATION_EVENT_RETRY_INTERVAL);
                    continue;
                }
                Err(TrySendError::Disconnected(_)) => break,
            }
        }
        let generation = control.generation();
        let outcome = replication.replicate_interruptible(OffsetDateTime::now_utc(), &|| {
            control.stopping.load(Ordering::Acquire)
        });
        let wait = replication_wait(&outcome);
        match outcome {
            Ok(RemoteReplicationOutcome::Cancelled) => break,
            Ok(outcome @ RemoteReplicationOutcome::Pushed { .. })
            | Ok(outcome @ RemoteReplicationOutcome::Failed { .. }) => {
                error_reported = false;
                pending_event = try_send_replication_event(events, Ok(outcome));
            }
            Ok(RemoteReplicationOutcome::UpToDate { .. })
            | Ok(RemoteReplicationOutcome::Deferred { .. }) => {
                error_reported = false;
            }
            Err(error) if !error_reported => {
                error_reported = true;
                pending_event = try_send_replication_event(events, Err(error));
            }
            Err(_) => {}
        }
        control.wait_after(
            generation,
            if pending_event.is_some() {
                REPLICATION_EVENT_RETRY_INTERVAL
            } else {
                wait
            },
        );
    }
}

fn try_send_replication_event(
    events: &SyncSender<ReplicationEvent>,
    event: ReplicationEvent,
) -> Option<ReplicationEvent> {
    match events.try_send(event) {
        Ok(()) => None,
        Err(TrySendError::Full(event)) => Some(event),
        Err(TrySendError::Disconnected(_)) => None,
    }
}

fn replication_wait(
    outcome: &Result<RemoteReplicationOutcome, RemoteReplicationError>,
) -> StandardDuration {
    let retry_at = match outcome {
        Ok(
            RemoteReplicationOutcome::Failed { retry_at, .. }
            | RemoteReplicationOutcome::Deferred { retry_at, .. },
        ) => Some(*retry_at),
        _ => None,
    };
    match retry_at.map(|retry_at| retry_at - OffsetDateTime::now_utc()) {
        Some(remaining) if remaining.is_positive() => StandardDuration::try_from(remaining)
            .unwrap_or(REPLICATION_VERIFICATION_INTERVAL)
            .min(REPLICATION_VERIFICATION_INTERVAL),
        Some(_) => StandardDuration::ZERO,
        None => REPLICATION_VERIFICATION_INTERVAL,
    }
}

/// Exclusive, recovered Worker state ready to process new batches.
#[derive(Debug)]
pub struct WorkerRuntime {
    session: WorkerSession,
    processor: BatchProcessor,
    limits: WorkerRunLimits,
    replication: Option<ReplicationBackground>,
    ready: bool,
}

impl WorkerRuntime {
    /// Acquires exclusive queue ownership and resolves interrupted durable work.
    ///
    /// # Errors
    ///
    /// Returns an error when queue recovery, repository discovery, transaction
    /// resumption, or safe pre-transaction requeueing fails.
    pub fn start(
        queue: &FileQueue,
        processor: BatchProcessor,
        limits: WorkerRunLimits,
        created_at: OffsetDateTime,
    ) -> Result<(Self, StartupOutcome), WorkerRunError> {
        match Self::start_interruptible(queue, processor, limits, created_at, &|| false)? {
            InterruptibleStart::Started(started) => Ok(started),
            InterruptibleStart::Stopped { .. } => {
                unreachable!("an always-running startup control cannot request shutdown")
            }
        }
    }

    /// Acquires exclusive ownership and allows shutdown between recovery steps.
    ///
    /// # Errors
    ///
    /// Returns an error when queue recovery, repository discovery, transaction
    /// resumption, or safe pre-transaction requeueing fails.
    /// Returns [`InterruptibleStart::Stopped`] when shutdown is requested; any
    /// durable partial claims remain safe for the next startup recovery.
    pub fn start_interruptible(
        queue: &FileQueue,
        processor: BatchProcessor,
        limits: WorkerRunLimits,
        created_at: OffsetDateTime,
        should_stop: &impl Fn() -> bool,
    ) -> Result<InterruptibleStart<(Self, StartupOutcome)>, WorkerRunError> {
        Self::start_interruptible_with_replication(
            queue,
            processor,
            None,
            limits,
            created_at,
            should_stop,
        )
    }

    pub(crate) fn start_interruptible_with_replication(
        queue: &FileQueue,
        processor: BatchProcessor,
        replication: Option<RemoteReplicator>,
        limits: WorkerRunLimits,
        created_at: OffsetDateTime,
        should_stop: &impl Fn() -> bool,
    ) -> Result<InterruptibleStart<(Self, StartupOutcome)>, WorkerRunError> {
        let mut session = queue.try_worker_session().map_err(WorkerRunError::queue)?;
        let summary = processor
            .unfinished_transaction_summary(&session)
            .map_err(|error| WorkerRunError::processor(error, 0))?;
        let summary_failures = summary.map_or(0, RepositoryTransaction::claim_failures);
        let Some(claims) = recover_processing(&mut session, limits, should_stop)
            .map_err(|error| error.with_failed_requests(summary_failures))?
        else {
            return Ok(InterruptibleStart::Stopped {
                failed_requests: summary_failures,
            });
        };
        let processing_batch = single_processing_batch(&claims)
            .map_err(|error| error.with_failed_requests(summary_failures))?;
        let transaction = processor
            .unfinished_transaction(&session)
            .map_err(|error| WorkerRunError::processor(error, summary_failures))?;
        let journal_failures = transaction.map_or(0, RepositoryTransaction::claim_failures);
        if should_stop() {
            return Ok(InterruptibleStart::Stopped {
                failed_requests: journal_failures,
            });
        }
        if let (Some(processing), Some(repository)) = (processing_batch, transaction)
            && processing != repository.batch_id()
        {
            return Err(WorkerRunError::TransactionBatchMismatch {
                processing,
                repository: repository.batch_id(),
                failed_requests: repository.claim_failures(),
            });
        }

        let startup = match (transaction, processing_batch) {
            (None, None) => StartupOutcome::Clean,
            (None, Some(batch_id)) => {
                let requests = claims.len();
                for claim in claims {
                    if should_stop() {
                        return Ok(InterruptibleStart::Stopped { failed_requests: 0 });
                    }
                    session
                        .requeue_claimed(claim.token())
                        .map_err(WorkerRunError::queue)?;
                }
                StartupOutcome::Requeued { batch_id, requests }
            }
            (
                Some(RepositoryTransaction::Preparing {
                    batch_id,
                    claim_failures,
                }),
                Some(_),
            ) => {
                if should_stop() {
                    return Ok(InterruptibleStart::Stopped {
                        failed_requests: claim_failures,
                    });
                }
                let outcome = processor
                    .process(&mut session, batch_id, &claims, claim_failures, created_at)
                    .map_err(|error| WorkerRunError::processor(error, claim_failures))?;
                StartupOutcome::Resumed {
                    batch_id,
                    outcome,
                    claim_failures,
                }
            }
            (Some(RepositoryTransaction::Preparing { batch_id, .. }), None) => {
                return Err(WorkerRunError::MissingProcessingClaims {
                    batch_id,
                    failed_requests: journal_failures,
                });
            }
            (
                Some(RepositoryTransaction::Recoverable {
                    batch_id,
                    claim_failures,
                }),
                _,
            ) => {
                if should_stop() {
                    return Ok(InterruptibleStart::Stopped {
                        failed_requests: claim_failures,
                    });
                }
                let outcome = processor
                    .recover(&mut session, batch_id, created_at)
                    .map_err(|error| WorkerRunError::processor(error, claim_failures))?;
                StartupOutcome::Resumed {
                    batch_id,
                    outcome,
                    claim_failures,
                }
            }
        };
        let replication = replication
            .map(ReplicationBackground::start)
            .transpose()
            .map_err(WorkerRunError::ReplicationThread)?;
        Ok(InterruptibleStart::Started((
            Self {
                session,
                processor,
                limits,
                replication,
                ready: true,
            },
            startup,
        )))
    }

    /// Takes the latest asynchronous Git remote replication event.
    ///
    /// The result is `None` when replication is not configured or no new
    /// reportable outcome is available. Background failures never change local
    /// request, commit, release, or queue processing state.
    pub fn take_replication_event(
        &self,
    ) -> Option<Result<RemoteReplicationOutcome, RemoteReplicationError>> {
        self.replication
            .as_ref()
            .and_then(ReplicationBackground::take_event)
    }

    /// Selects the earliest bounded pending snapshot and processes one batch.
    ///
    /// The caller decides when batching thresholds are satisfied before
    /// invoking this method. An empty snapshot returns [`WorkerRunOutcome::Idle`].
    ///
    /// # Errors
    ///
    /// Returns an error when selection, claiming, repository application, or
    /// publication fails. Durable state is retained for the next startup.
    pub fn run_once(
        &mut self,
        created_at: OffsetDateTime,
    ) -> Result<WorkerRunOutcome, WorkerRunError> {
        if !self.ready {
            return Err(WorkerRunError::RecoveryRequired);
        }
        self.session.discard_pending_observation();
        self.ready = false;
        let outcome = self.run_once_inner(created_at, None, &|| false);
        if outcome.is_ok() {
            self.ready = true;
        }
        match outcome? {
            WorkerRunProgress::Stopped { .. } => {
                unreachable!("an always-running cycle control cannot request shutdown")
            }
            WorkerRunProgress::Complete(outcome) => Ok(outcome),
        }
    }

    /// Observes pending work once and processes it only when a threshold closes.
    ///
    /// Repeated calls incrementally finish a fixed acceptance snapshot. A
    /// ready snapshot is claimed only through its observed sequence boundary,
    /// so requests arriving after observation remain in the next batch.
    ///
    /// # Errors
    ///
    /// Returns an error when observation, claiming, repository application, or
    /// publication fails. A failed processing cycle requires startup recovery.
    pub fn poll_once(
        &mut self,
        schedule: BatchSchedule,
        now: OffsetDateTime,
    ) -> Result<WorkerPollOutcome, WorkerRunError> {
        match self.poll_once_interruptible(schedule, now, &|| false)? {
            WorkerPollOutcome::Stopped { .. } => {
                unreachable!("an always-running poll control cannot request shutdown")
            }
            outcome => Ok(outcome),
        }
    }

    /// Polls one bounded scheduler step and observes pre-transaction shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error when observation, claiming, repository application, or
    /// publication fails. A failed processing cycle requires startup recovery.
    /// [`WorkerPollOutcome::Stopped`] is also terminal for this runtime.
    pub fn poll_once_interruptible(
        &mut self,
        schedule: BatchSchedule,
        now: OffsetDateTime,
        should_stop: &impl Fn() -> bool,
    ) -> Result<WorkerPollOutcome, WorkerRunError> {
        if !self.ready {
            return Err(WorkerRunError::RecoveryRequired);
        }
        if should_stop() {
            return Ok(self.stop(0));
        }
        let snapshot = match self
            .session
            .scan_pending(self.limits.maximum_scan_entries.get())
            .map_err(WorkerRunError::queue)?
        {
            PendingScanOutcome::Scanning {
                scanned_entries,
                observed_requests,
            } => {
                if should_stop() {
                    return Ok(self.stop(0));
                }
                return Ok(WorkerPollOutcome::Scanning {
                    scanned_entries,
                    observed_requests,
                });
            }
            PendingScanOutcome::Complete(snapshot) => snapshot,
        };
        if should_stop() {
            return Ok(self.stop(0));
        }
        match schedule.readiness(snapshot, self.limits.maximum_requests, now) {
            BatchReadiness::Empty => Ok(WorkerPollOutcome::Idle),
            BatchReadiness::Waiting { ready_at } => Ok(WorkerPollOutcome::Waiting { ready_at }),
            BatchReadiness::Ready { reason } => {
                self.ready = false;
                let outcome =
                    self.run_once_inner(now, Some(snapshot.maximum_sequence()), should_stop);
                if outcome.is_ok() {
                    self.ready = true;
                }
                match outcome? {
                    WorkerRunProgress::Stopped { claim_failures } => Ok(self.stop(claim_failures)),
                    WorkerRunProgress::Complete(WorkerRunOutcome::Idle) => {
                        Ok(WorkerPollOutcome::ClosedWithoutCommit {
                            reason,
                            failed_requests: 0,
                        })
                    }
                    WorkerRunProgress::Complete(WorkerRunOutcome::ClosedWithoutProcessing {
                        failed_requests,
                    }) => Ok(WorkerPollOutcome::ClosedWithoutCommit {
                        reason,
                        failed_requests,
                    }),
                    WorkerRunProgress::Complete(WorkerRunOutcome::Processed {
                        batch_id,
                        outcome,
                        claim_failures,
                    }) => Ok(WorkerPollOutcome::Processed {
                        reason,
                        batch_id,
                        outcome,
                        claim_failures,
                    }),
                }
            }
        }
    }

    fn run_once_inner(
        &mut self,
        created_at: OffsetDateTime,
        maximum_sequence: Option<u64>,
        should_stop: &impl Fn() -> bool,
    ) -> Result<WorkerRunProgress, WorkerRunError> {
        let batch_id = BatchId::generate();
        let (claims, claim_failures) = loop {
            let outcome = match maximum_sequence {
                Some(maximum_sequence) => self.session.claim_batch_through(
                    batch_id,
                    maximum_sequence,
                    self.limits.maximum_scan_entries.get(),
                    self.limits.maximum_requests.get(),
                ),
                None => self.session.claim_next_batch(
                    batch_id,
                    self.limits.maximum_scan_entries.get(),
                    self.limits.maximum_requests.get(),
                ),
            };
            match outcome {
                Ok(BatchClaimOutcome::Scanning {
                    failed_requests, ..
                }) => {
                    if should_stop() {
                        return Ok(WorkerRunProgress::Stopped {
                            claim_failures: failed_requests,
                        });
                    }
                }
                Ok(BatchClaimOutcome::Claimed {
                    claims,
                    failed_requests,
                }) => break (claims, failed_requests),
                Err(error) => return Err(WorkerRunError::queue(error)),
            }
        };
        if should_stop() {
            return Ok(WorkerRunProgress::Stopped { claim_failures });
        }
        if claims.is_empty() {
            let outcome = if claim_failures == 0 {
                WorkerRunOutcome::Idle
            } else {
                WorkerRunOutcome::ClosedWithoutProcessing {
                    failed_requests: claim_failures,
                }
            };
            return Ok(WorkerRunProgress::Complete(outcome));
        }
        let outcome = self
            .processor
            .process(
                &mut self.session,
                batch_id,
                &claims,
                claim_failures,
                created_at,
            )
            .map_err(|error| WorkerRunError::processor(error, claim_failures))?;
        if let Some(replication) = &self.replication {
            replication.wake();
        }
        Ok(WorkerRunProgress::Complete(WorkerRunOutcome::Processed {
            batch_id,
            outcome,
            claim_failures,
        }))
    }

    fn stop(&mut self, failed_requests: usize) -> WorkerPollOutcome {
        self.ready = false;
        WorkerPollOutcome::Stopped { failed_requests }
    }
}

enum WorkerRunProgress {
    Stopped { claim_failures: usize },
    Complete(WorkerRunOutcome),
}

fn recover_processing(
    session: &mut WorkerSession,
    limits: WorkerRunLimits,
    should_stop: &impl Fn() -> bool,
) -> Result<Option<Vec<ClaimedPackage>>, WorkerRunError> {
    let mut recovered = Vec::new();
    loop {
        let remaining_with_sentinel = limits
            .maximum_recovery_requests
            .get()
            .saturating_sub(recovered.len())
            .saturating_add(1);
        let scan_entries = limits
            .maximum_scan_entries
            .get()
            .min(remaining_with_sentinel);
        let (claims, complete) = match session
            .scan_processing(scan_entries)
            .map_err(WorkerRunError::queue)?
        {
            ProcessingScanOutcome::Scanning { claims, .. } => (claims, false),
            ProcessingScanOutcome::Complete { claims, .. } => (claims, true),
        };
        recovered.extend(claims);
        if recovered.len() > limits.maximum_recovery_requests.get() {
            return Err(WorkerRunError::TooManyProcessingClaims {
                maximum: limits.maximum_recovery_requests.get(),
            });
        }
        if should_stop() {
            return Ok(None);
        }
        if complete {
            recovered.sort_by_key(|claim| {
                claim
                    .package()
                    .acceptance()
                    .map(|acceptance| acceptance.sequence)
            });
            return Ok(Some(recovered));
        }
    }
}

fn single_processing_batch(claims: &[ClaimedPackage]) -> Result<Option<BatchId>, WorkerRunError> {
    let Some(first) = claims.first() else {
        return Ok(None);
    };
    let expected = first.token().batch_id();
    for claim in &claims[1..] {
        let found = claim.token().batch_id();
        if found != expected {
            return Err(WorkerRunError::MultipleProcessingBatches { expected, found });
        }
    }
    Ok(Some(expected))
}

/// Failure while starting or processing one bounded Worker cycle.
#[derive(Debug)]
pub enum WorkerRunError {
    /// Durable queue discovery or transition failed.
    Queue(Box<WorkerQueueError>),
    /// Repository application or publication failed.
    Processor {
        /// Concrete batch-processing failure.
        source: Box<BatchProcessorError>,
        /// Requests rejected before repository processing began.
        failed_requests: usize,
    },
    /// Startup recovery failed after a durable journal count was discovered.
    StartupRecovery {
        /// Concrete recovery failure.
        source: Box<WorkerRunError>,
        /// Requests rejected before startup recovery began.
        failed_requests: usize,
    },
    /// The independent Git replication thread could not be started.
    ReplicationThread(io::Error),
    /// Interrupted processing exceeded the configured in-memory batch bound.
    TooManyProcessingClaims {
        /// Configured maximum number of claims.
        maximum: usize,
    },
    /// Interrupted claims unexpectedly belonged to multiple batches.
    MultipleProcessingBatches {
        /// Batch found first.
        expected: BatchId,
        /// Conflicting batch found later.
        found: BatchId,
    },
    /// Queue claims and the repository journal named different batches.
    TransactionBatchMismatch {
        /// Batch found in processing queue records.
        processing: BatchId,
        /// Batch found in the repository journal.
        repository: BatchId,
        /// Requests rejected before the repository transaction began.
        failed_requests: usize,
    },
    /// A preparing repository transaction had no live claims to replay.
    MissingProcessingClaims {
        /// Batch named by the repository journal.
        batch_id: BatchId,
        /// Requests rejected before the repository transaction began.
        failed_requests: usize,
    },
    /// A previous cycle failed and startup recovery is required before reuse.
    RecoveryRequired,
}

impl WorkerRunError {
    fn queue(error: WorkerQueueError) -> Self {
        Self::Queue(Box::new(error))
    }

    fn processor(error: BatchProcessorError, failed_requests: usize) -> Self {
        Self::Processor {
            source: Box::new(error),
            failed_requests,
        }
    }

    fn with_failed_requests(self, failed_requests: usize) -> Self {
        if failed_requests == 0 || self.failed_requests() > 0 {
            self
        } else {
            Self::StartupRecovery {
                source: Box::new(self),
                failed_requests,
            }
        }
    }

    /// Returns requests durably rejected before this cycle failed.
    #[must_use]
    pub const fn failed_requests(&self) -> usize {
        match self {
            Self::Queue(error) => error.failed_requests(),
            Self::Processor {
                failed_requests, ..
            } => *failed_requests,
            Self::StartupRecovery {
                failed_requests, ..
            } => *failed_requests,
            Self::TransactionBatchMismatch {
                failed_requests, ..
            }
            | Self::MissingProcessingClaims {
                failed_requests, ..
            } => *failed_requests,
            _ => 0,
        }
    }
}

impl From<BatchProcessorError> for WorkerRunError {
    fn from(error: BatchProcessorError) -> Self {
        Self::processor(error, 0)
    }
}

impl fmt::Display for WorkerRunError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Queue(error) => write!(formatter, "Worker queue operation failed: {error}"),
            Self::Processor { source, .. } => {
                write!(formatter, "Worker batch processing failed: {source}")
            }
            Self::StartupRecovery { source, .. } => {
                write!(formatter, "Worker startup recovery failed: {source}")
            }
            Self::ReplicationThread(error) => {
                write!(formatter, "Git replication thread could not start: {error}")
            }
            Self::TooManyProcessingClaims { maximum } => write!(
                formatter,
                "interrupted processing exceeds the configured {maximum}-request bound"
            ),
            Self::MultipleProcessingBatches { expected, found } => write!(
                formatter,
                "interrupted processing names batches `{expected}` and `{found}`"
            ),
            Self::TransactionBatchMismatch {
                processing,
                repository,
                ..
            } => write!(
                formatter,
                "processing batch `{processing}` does not match repository batch `{repository}`"
            ),
            Self::MissingProcessingClaims { batch_id, .. } => write!(
                formatter,
                "preparing repository batch `{batch_id}` has no processing claims"
            ),
            Self::RecoveryRequired => formatter.write_str(
                "Worker runtime requires startup recovery after a failed processing cycle",
            ),
        }
    }
}

impl std::error::Error for WorkerRunError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Queue(error) => Some(error),
            Self::Processor { source, .. } => Some(source),
            Self::StartupRecovery { source, .. } => Some(source),
            Self::ReplicationThread(error) => Some(error),
            Self::TooManyProcessingClaims { .. }
            | Self::MultipleProcessingBatches { .. }
            | Self::TransactionBatchMismatch { .. }
            | Self::MissingProcessingClaims { .. }
            | Self::RecoveryRequired => None,
        }
    }
}

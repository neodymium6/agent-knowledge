//! Repository Worker batch publication and crash recovery.

use std::cell::{Cell, RefCell};
use std::fmt;
use std::path::Path;

use agent_knowledge_core::{BatchId, ErrorCode};
use agent_knowledge_queue::{ClaimToken, ClaimedPackage, PackagePolicy, WorkerSession};
use agent_knowledge_release::{
    ActiveRelease, BuiltDirectory, PreparedRelease, QuartzBuildError, QuartzBuilder, ReleaseError,
    ReleaseStore,
};
use agent_knowledge_repository::{
    ActiveSearchIndex, BatchPublication, ClaimedBatch, CommittedReadError, CommittedStore,
    ContentPolicy, GitRepository, GitTransactionError, PublicationError, RepositoryTransaction,
    RequestFailure, SearchIndexStore, SearchIndexStoreError, SearchMetadataFields,
};
pub use agent_knowledge_repository::{
    BatchCommitOutcome, RemoteReplicationError, RemoteReplicationOutcome, RemoteReplicationPolicy,
};
use time::OffsetDateTime;

mod config;
mod maintenance;
pub use config::{CURRENT_WORKER_CONFIG_VERSION, WorkerConfigError, WorkerSettings};
pub use maintenance::{ReleaseMaintenanceError, retain_derived_releases};
mod bootstrap;
pub use bootstrap::{WorkerBootstrap, WorkerOpenError};
mod status;
pub use status::{
    ActiveReleaseStatus, OperationalStatus, OperationalStatusError, PublicationStatus, QueueStatus,
    ReplicationStatus, inspect_operational_status,
};

/// Connects one queue session, Git transaction, Quartz build, and release.
#[derive(Clone, Debug)]
pub struct BatchProcessor {
    repository: GitRepository,
    quartz: QuartzBuilder,
    releases: ReleaseStore,
    content_policy: ContentPolicy,
    package_policy: PackagePolicy,
    search: Option<SearchPublication>,
}

#[derive(Clone, Debug)]
struct SearchPublication {
    committed: CommittedStore,
    indexes: SearchIndexStore,
}

impl BatchProcessor {
    /// Creates a processor from already validated, lifetime-pinned components.
    #[must_use]
    pub const fn new(
        repository: GitRepository,
        quartz: QuartzBuilder,
        releases: ReleaseStore,
        content_policy: ContentPolicy,
        package_policy: PackagePolicy,
    ) -> Self {
        Self {
            repository,
            quartz,
            releases,
            content_policy,
            package_policy,
            search: None,
        }
    }

    /// Enables committed-snapshot search index publication for this processor.
    #[must_use]
    pub fn with_search_index(
        mut self,
        committed: CommittedStore,
        indexes: SearchIndexStore,
    ) -> Self {
        self.search = Some(SearchPublication { committed, indexes });
        self
    }

    /// Discovers durable repository work that must be resumed at startup.
    ///
    /// # Errors
    ///
    /// Returns an error when the repository journal or its queue binding is
    /// invalid.
    pub fn unfinished_transaction(
        &self,
        worker: &WorkerSession,
    ) -> Result<Option<RepositoryTransaction>, BatchProcessorError> {
        self.repository
            .unfinished_transaction(worker)
            .map_err(BatchProcessorError::repository)
    }

    /// Reads durable transaction metadata before queue recovery completes.
    ///
    /// This supports accurate interruption and failure reporting only. A
    /// caller must still use [`Self::unfinished_transaction`] after queue
    /// recovery before replaying repository state.
    ///
    /// # Errors
    ///
    /// Returns an error when the repository journal or queue binding is invalid.
    pub fn unfinished_transaction_summary(
        &self,
        worker: &WorkerSession,
    ) -> Result<Option<RepositoryTransaction>, BatchProcessorError> {
        self.repository
            .unfinished_transaction_summary(worker)
            .map_err(BatchProcessorError::repository)
    }

    /// Applies, publishes, reconciles, and finalizes one claimed batch.
    ///
    /// # Errors
    ///
    /// Returns an error when repository application, Quartz execution,
    /// release publication, queue reconciliation, or journal finalization
    /// fails. A durable journal is intentionally retained whenever recovery
    /// is still required.
    pub fn process(
        &self,
        worker: &mut WorkerSession,
        batch_id: BatchId,
        claims: &[ClaimedPackage],
        claim_failures: usize,
        created_at: OffsetDateTime,
    ) -> Result<BatchCommitOutcome, BatchProcessorError> {
        let built = RefCell::new(None::<BuiltDirectory>);
        let prepared = RefCell::new(None::<PreparedRelease>);
        let callback_error = RefCell::new(None::<BatchProcessorError>);
        let trial_failed = Cell::new(false);
        let outcome = self.repository.apply_batch_with_publication(
            worker,
            batch_id,
            ClaimedBatch::new(claims, claim_failures),
            self.content_policy,
            &self.package_policy,
            BatchPublication::new(
                |content: &Path| {
                    let result = self.build(batch_id, content);
                    match result {
                        Ok(output) => {
                            built.replace(Some(output));
                            Ok(())
                        }
                        Err(error) => {
                            trial_failed.set(true);
                            callback_error.replace(Some(error));
                            Err(PublicationError::new())
                        }
                    }
                },
                |_: &Path, commit: &str| {
                    let Some(output) = built.take() else {
                        callback_error.replace(Some(BatchProcessorError::MissingBuildOutput));
                        return Err(PublicationError::new());
                    };
                    match self.releases.prepare(output, commit, created_at) {
                        Ok(release) => {
                            prepared.replace(Some(release));
                            Ok(())
                        }
                        Err(error) => {
                            callback_error.replace(Some(BatchProcessorError::release(error)));
                            Err(PublicationError::new())
                        }
                    }
                },
            ),
        );
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                let error = callback_error
                    .take()
                    .unwrap_or_else(|| BatchProcessorError::repository(error));
                let has_unconsumed_build = built.take().is_some();
                if trial_failed.get() || has_unconsumed_build {
                    self.repository
                        .abort_preparing_batch(worker, batch_id, claims)
                        .map_err(BatchProcessorError::repository)?;
                    self.releases
                        .discard_build(batch_id)
                        .map_err(BatchProcessorError::release)?;
                }
                return Err(error);
            }
        };
        let active = match &outcome {
            BatchCommitOutcome::Committed { commit, .. } => {
                self.publish_search_index(commit)?;
                let release = match prepared.take() {
                    Some(release) => release,
                    None => self
                        .releases
                        .prepared_for_commit(commit)
                        .map_err(BatchProcessorError::release)?
                        .ok_or(BatchProcessorError::MissingPreparedRelease)?,
                };
                Some(
                    self.releases
                        .activate(&release)
                        .map_err(BatchProcessorError::release)?,
                )
            }
            BatchCommitOutcome::NoChanges { .. } => None,
        };
        self.reconcile_and_finalize(worker, batch_id, &outcome, active.as_ref())?;
        Ok(outcome)
    }

    /// Resumes a terminal repository journal and completes publication.
    ///
    /// # Errors
    ///
    /// Returns an error when the journal cannot be recovered or its exact
    /// release and queue outcomes cannot be durably completed.
    pub fn recover(
        &self,
        worker: &mut WorkerSession,
        batch_id: BatchId,
        created_at: OffsetDateTime,
    ) -> Result<BatchCommitOutcome, BatchProcessorError> {
        let prepared = RefCell::new(None::<PreparedRelease>);
        let callback_error = RefCell::new(None::<BatchProcessorError>);
        let outcome =
            self.repository
                .recover_batch_with_publication(worker, batch_id, |content, commit| {
                    match self.prepare_recovered(batch_id, content, commit, created_at) {
                        Ok(release) => {
                            prepared.replace(Some(release));
                            Ok(())
                        }
                        Err(error) => {
                            callback_error.replace(Some(error));
                            Err(PublicationError::new())
                        }
                    }
                });
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                return Err(callback_error
                    .take()
                    .unwrap_or_else(|| BatchProcessorError::repository(error)));
            }
        };
        let active = match &outcome {
            BatchCommitOutcome::Committed { commit, .. } => {
                self.publish_search_index(commit)?;
                Some(self.activate_recovered(batch_id, commit, prepared.take())?)
            }
            BatchCommitOutcome::NoChanges { .. } => None,
        };
        self.reconcile_and_finalize(worker, batch_id, &outcome, active.as_ref())?;
        Ok(outcome)
    }

    fn build(
        &self,
        batch_id: BatchId,
        content: &Path,
    ) -> Result<BuiltDirectory, BatchProcessorError> {
        let build = self
            .releases
            .begin_build(batch_id)
            .map_err(BatchProcessorError::release)?;
        self.quartz
            .build(content, build)
            .map_err(BatchProcessorError::quartz)
    }

    pub(crate) fn ensure_current_search_index(
        &self,
    ) -> Result<Option<ActiveSearchIndex>, BatchProcessorError> {
        let Some(search) = self.search.as_ref() else {
            return Ok(None);
        };
        let snapshot = search
            .committed
            .snapshot(self.content_policy, &self.package_policy)
            .map_err(BatchProcessorError::committed_read)?;
        self.publish_search_snapshot(search, &snapshot).map(Some)
    }

    fn publish_search_index(
        &self,
        expected_commit: &str,
    ) -> Result<Option<ActiveSearchIndex>, BatchProcessorError> {
        let Some(search) = self.search.as_ref() else {
            return Ok(None);
        };
        if let Some(active) = search
            .indexes
            .active_index()
            .map_err(BatchProcessorError::search_index)?
            && active.commit() == expected_commit
        {
            return Ok(Some(active));
        }
        let snapshot = search
            .committed
            .snapshot(self.content_policy, &self.package_policy)
            .map_err(BatchProcessorError::committed_read)?;
        if snapshot.commit() != expected_commit {
            return Err(BatchProcessorError::SearchSnapshotCommitMismatch {
                expected: expected_commit.to_owned(),
                actual: snapshot.commit().to_owned(),
            });
        }
        self.publish_search_snapshot(search, &snapshot).map(Some)
    }

    fn publish_search_snapshot(
        &self,
        search: &SearchPublication,
        snapshot: &agent_knowledge_repository::CommittedSnapshot,
    ) -> Result<ActiveSearchIndex, BatchProcessorError> {
        let prepared = search
            .indexes
            .prepare(snapshot, SearchMetadataFields::default())
            .map_err(BatchProcessorError::search_index)?;
        search
            .indexes
            .activate(&prepared)
            .map_err(BatchProcessorError::search_index)
    }

    fn prepare_recovered(
        &self,
        batch_id: BatchId,
        content: &Path,
        commit: &str,
        created_at: OffsetDateTime,
    ) -> Result<PreparedRelease, BatchProcessorError> {
        match self.releases.resume_prepare(batch_id, commit) {
            Ok(prepared) => Ok(prepared),
            Err(ReleaseError::MissingRecoveryState) => {
                self.releases
                    .discard_build(batch_id)
                    .map_err(BatchProcessorError::release)?;
                let built = self.build(batch_id, content)?;
                self.releases
                    .prepare(built, commit, created_at)
                    .map_err(BatchProcessorError::release)
            }
            Err(error) => Err(BatchProcessorError::release(error)),
        }
    }

    fn activate_recovered(
        &self,
        batch_id: BatchId,
        commit: &str,
        prepared: Option<PreparedRelease>,
    ) -> Result<ActiveRelease, BatchProcessorError> {
        if let Some(active) = self
            .releases
            .active_release()
            .map_err(BatchProcessorError::release)?
            && active.commit() == commit
        {
            return Ok(active);
        }
        let prepared = match prepared {
            Some(prepared) => prepared,
            None => self
                .releases
                .resume_prepare(batch_id, commit)
                .map_err(BatchProcessorError::release)?,
        };
        self.releases
            .activate(&prepared)
            .map_err(BatchProcessorError::release)
    }

    fn reconcile_and_finalize(
        &self,
        worker: &mut WorkerSession,
        batch_id: BatchId,
        outcome: &BatchCommitOutcome,
        active: Option<&ActiveRelease>,
    ) -> Result<(), BatchProcessorError> {
        let (successful, failures) = outcome_tokens(outcome);
        let reconciliation = worker
            .reconcile_batch(batch_id, successful, &failures)
            .map_err(BatchProcessorError::queue)?;
        self.repository
            .finalize_batch(
                worker,
                batch_id,
                outcome,
                &reconciliation,
                active.map(|_| &self.releases),
            )
            .map_err(BatchProcessorError::repository)
    }
}

mod runtime;
pub use runtime::{
    InterruptibleStart, ReplicationEventError, StartupOutcome, WorkerPollOutcome, WorkerRunError,
    WorkerRunLimits, WorkerRunOutcome, WorkerRuntime,
};
mod schedule;
pub use schedule::{BatchCloseReason, BatchReadiness, BatchSchedule, BatchScheduleError};

fn outcome_tokens(outcome: &BatchCommitOutcome) -> (&[ClaimToken], Vec<(ClaimToken, ErrorCode)>) {
    match outcome {
        BatchCommitOutcome::NoChanges { failures } => (&[], failure_tokens(failures)),
        BatchCommitOutcome::Committed {
            successful,
            failures,
            ..
        } => (successful, failure_tokens(failures)),
    }
}

fn failure_tokens(failures: &[RequestFailure]) -> Vec<(ClaimToken, ErrorCode)> {
    failures
        .iter()
        .map(|failure| (failure.token(), failure.error_code()))
        .collect()
}

/// Failure while completing or recovering one Worker batch.
#[derive(Debug)]
pub enum BatchProcessorError {
    /// Repository transaction or journal validation failed.
    Repository(Box<GitTransactionError>),
    /// Quartz execution failed.
    Quartz(Box<QuartzBuildError>),
    /// Immutable release preparation or activation failed.
    Release(Box<ReleaseError>),
    /// The canonical committed snapshot could not be opened for indexing.
    CommittedRead(Box<CommittedReadError>),
    /// Persistent search index publication failed.
    SearchIndex(Box<SearchIndexStoreError>),
    /// A terminal queue transition failed.
    Queue(Box<agent_knowledge_queue::WorkerQueueError>),
    /// The repository requested publication without a successful trial output.
    MissingBuildOutput,
    /// A committed batch had no recoverable prepared release.
    MissingPreparedRelease,
    /// The canonical committed snapshot did not match the completed batch.
    SearchSnapshotCommitMismatch {
        /// Commit produced by the completed repository transaction.
        expected: String,
        /// Commit observed from the canonical read boundary.
        actual: String,
    },
}

impl BatchProcessorError {
    fn repository(error: GitTransactionError) -> Self {
        Self::Repository(Box::new(error))
    }

    fn quartz(error: QuartzBuildError) -> Self {
        Self::Quartz(Box::new(error))
    }

    fn release(error: ReleaseError) -> Self {
        Self::Release(Box::new(error))
    }

    fn committed_read(error: CommittedReadError) -> Self {
        Self::CommittedRead(Box::new(error))
    }

    fn search_index(error: SearchIndexStoreError) -> Self {
        Self::SearchIndex(Box::new(error))
    }

    fn queue(error: agent_knowledge_queue::WorkerQueueError) -> Self {
        Self::Queue(Box::new(error))
    }
}

impl fmt::Display for BatchProcessorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Repository(error) => write!(formatter, "repository transaction failed: {error}"),
            Self::Quartz(error) => write!(formatter, "Quartz build failed: {error}"),
            Self::Release(error) => write!(formatter, "release publication failed: {error}"),
            Self::CommittedRead(error) => {
                write!(formatter, "committed search snapshot failed: {error}")
            }
            Self::SearchIndex(error) => {
                write!(formatter, "search index publication failed: {error}")
            }
            Self::Queue(error) => {
                write!(formatter, "terminal queue reconciliation failed: {error}")
            }
            Self::MissingBuildOutput => {
                formatter.write_str("repository publication has no successful Quartz output")
            }
            Self::MissingPreparedRelease => {
                formatter.write_str("committed batch has no recoverable prepared release")
            }
            Self::SearchSnapshotCommitMismatch { expected, actual } => write!(
                formatter,
                "committed search snapshot `{actual}` does not match batch commit `{expected}`"
            ),
        }
    }
}

impl std::error::Error for BatchProcessorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Repository(error) => Some(error),
            Self::Quartz(error) => Some(error),
            Self::Release(error) => Some(error),
            Self::CommittedRead(error) => Some(error),
            Self::SearchIndex(error) => Some(error),
            Self::Queue(error) => Some(error),
            Self::MissingBuildOutput
            | Self::MissingPreparedRelease
            | Self::SearchSnapshotCommitMismatch { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests;

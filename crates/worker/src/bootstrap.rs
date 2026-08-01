use std::fmt;

use agent_knowledge_queue::{FileQueue, PackagePolicy, QueueError};
use agent_knowledge_release::{
    QuartzBuildError, QuartzBuilder, ReleaseError, ReleasePolicy, ReleaseStore,
};
use agent_knowledge_repository::{ContentPolicy, GitRepository, GitTransactionError};
use time::OffsetDateTime;

use crate::{
    BatchProcessor, BatchSchedule, StartupOutcome, WorkerRunError, WorkerRunLimits, WorkerRuntime,
    WorkerSettings,
};

/// Lifetime-pinned Worker dependencies ready for startup recovery.
#[derive(Debug)]
pub struct WorkerBootstrap {
    queue: FileQueue,
    processor: BatchProcessor,
    limits: WorkerRunLimits,
    schedule: BatchSchedule,
}

impl WorkerBootstrap {
    /// Opens and pins every configured Worker dependency.
    ///
    /// Repository and Quartz inputs are validated before queue initialization,
    /// preventing an unusable configuration from creating a live queue first.
    ///
    /// # Errors
    ///
    /// Returns the exact component boundary that could not be opened.
    pub fn open(settings: WorkerSettings) -> Result<Self, WorkerOpenError> {
        let repository = GitRepository::open(
            settings.repository_root(),
            settings.content_root(),
            settings.work_root(),
            settings.official_branch(),
            settings.identity().clone(),
        )
        .map_err(|error| WorkerOpenError::Repository(Box::new(error)))?;
        let release_policy = ReleasePolicy::default();
        let quartz = QuartzBuilder::new_with_policy(
            settings.quartz_program(),
            settings.quartz_integration_root(),
            settings.quartz_arguments().to_vec(),
            settings.quartz_timeout(),
            release_policy,
        )
        .map_err(|error| WorkerOpenError::Quartz(Box::new(error)))?;
        let releases = ReleaseStore::open(settings.release_root(), release_policy)
            .map_err(|error| WorkerOpenError::Release(Box::new(error)))?;
        let package_policy = PackagePolicy::default();
        let queue = FileQueue::initialize(settings.queue_root(), package_policy.clone())
            .map_err(|error| WorkerOpenError::Queue(Box::new(error)))?;
        let processor = BatchProcessor::new(
            repository,
            quartz,
            releases,
            ContentPolicy::default(),
            package_policy,
        );
        Ok(Self {
            queue,
            processor,
            limits: settings.limits(),
            schedule: settings.schedule(),
        })
    }

    /// Runs startup recovery and returns the ready runtime and its schedule.
    ///
    /// # Errors
    ///
    /// Returns an error when interrupted durable work cannot be recovered.
    pub fn start(
        self,
        created_at: OffsetDateTime,
    ) -> Result<(WorkerRuntime, StartupOutcome, BatchSchedule), WorkerRunError> {
        let (runtime, startup) =
            WorkerRuntime::start(&self.queue, self.processor, self.limits, created_at)?;
        Ok((runtime, startup, self.schedule))
    }
}

/// Failure while opening lifetime-pinned Worker components.
#[derive(Debug)]
pub enum WorkerOpenError {
    /// The repository transaction boundary was invalid.
    Repository(Box<GitTransactionError>),
    /// The configured Quartz command could not be pinned.
    Quartz(Box<QuartzBuildError>),
    /// The release store could not be opened.
    Release(Box<ReleaseError>),
    /// The durable queue could not be initialized.
    Queue(Box<QueueError>),
}

impl fmt::Display for WorkerOpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Repository(error) => write!(formatter, "could not open repository: {error}"),
            Self::Quartz(error) => write!(formatter, "could not open Quartz builder: {error}"),
            Self::Release(error) => write!(formatter, "could not open release store: {error}"),
            Self::Queue(error) => write!(formatter, "could not open durable queue: {error}"),
        }
    }
}

impl std::error::Error for WorkerOpenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Repository(error) => Some(error),
            Self::Quartz(error) => Some(error),
            Self::Release(error) => Some(error),
            Self::Queue(error) => Some(error),
        }
    }
}

#[cfg(test)]
mod tests;

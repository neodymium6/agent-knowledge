use std::ffi::OsString;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use agent_knowledge_core::{PathAttestation, PathAttestationError};
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
        validate_resolved_topology(&settings)?;
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
            settings.quartz_timeout(),
            release_policy,
        )
        .map_err(|error| WorkerOpenError::Quartz(Box::new(error)))?;
        let releases = ReleaseStore::open(settings.release_root(), release_policy)
            .map_err(|error| WorkerOpenError::Release(Box::new(error)))?;
        let package_policy = PackagePolicy::default();
        let queue = FileQueue::initialize(settings.queue_root(), package_policy.clone())
            .map_err(|error| WorkerOpenError::Queue(Box::new(error)))?;
        validate_opened_topology(&repository, &quartz, &releases, &queue)?;
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
    /// A configured path could not be resolved against the live filesystem.
    PathResolution {
        /// Configuration field whose path could not be resolved.
        field: &'static str,
        /// Filesystem error returned while resolving the path.
        source: io::Error,
    },
    /// Two configured paths resolve to equal or nested filesystem locations.
    OverlappingPaths {
        /// First conflicting configuration field.
        first: &'static str,
        /// Second conflicting configuration field.
        second: &'static str,
    },
    /// A component could not attest the filesystem object that it pinned.
    Attestation {
        /// Component boundary whose pinned path could not be attested.
        component: &'static str,
        /// Attestation failure.
        source: PathAttestationError,
    },
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
            Self::PathResolution { field, source } => {
                write!(
                    formatter,
                    "could not resolve Worker path `{field}`: {source}"
                )
            }
            Self::OverlappingPaths { first, second } => write!(
                formatter,
                "Worker paths `{first}` and `{second}` resolve to overlapping locations"
            ),
            Self::Attestation { component, source } => {
                write!(formatter, "could not attest {component}: {source}")
            }
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
            Self::PathResolution { source, .. } => Some(source),
            Self::OverlappingPaths { .. } => None,
            Self::Attestation { source, .. } => Some(source),
            Self::Repository(error) => Some(error),
            Self::Quartz(error) => Some(error),
            Self::Release(error) => Some(error),
            Self::Queue(error) => Some(error),
        }
    }
}

fn validate_opened_topology(
    repository: &GitRepository,
    quartz: &QuartzBuilder,
    releases: &ReleaseStore,
    queue: &FileQueue,
) -> Result<(), WorkerOpenError> {
    let [repository_root, content_root, work_root] =
        repository
            .storage_attestations()
            .map_err(|source| WorkerOpenError::Attestation {
                component: "repository storage",
                source,
            })?;
    let release_root =
        releases
            .storage_attestation()
            .map_err(|source| WorkerOpenError::Attestation {
                component: "release storage",
                source,
            })?;
    let queue_root =
        queue
            .storage_attestation()
            .map_err(|source| WorkerOpenError::Attestation {
                component: "queue storage",
                source,
            })?;
    let [quartz_program, quartz_integration] =
        quartz
            .trusted_attestations()
            .map_err(|source| WorkerOpenError::Attestation {
                component: "Quartz command",
                source,
            })?;
    let storage = [
        ("storage.queue_root", queue_root),
        ("storage.repository_root", repository_root),
        ("storage.content_root", content_root),
        ("storage.work_root", work_root),
        ("storage.release_root", release_root),
    ];
    for (index, (field, attestation)) in storage.iter().enumerate() {
        for (other_field, other_attestation) in &storage[index + 1..] {
            reject_attested_overlap(field, attestation, other_field, other_attestation)?;
        }
    }
    let trusted = [
        ("quartz.program", quartz_program),
        ("quartz.integration_root", quartz_integration),
    ];
    for (storage_field, storage_attestation) in &storage {
        for (trusted_field, trusted_attestation) in &trusted {
            reject_attested_overlap(
                storage_field,
                storage_attestation,
                trusted_field,
                trusted_attestation,
            )?;
        }
    }
    Ok(())
}

fn validate_resolved_topology(settings: &WorkerSettings) -> Result<(), WorkerOpenError> {
    let storage = [
        (
            "storage.queue_root",
            resolve_destination("storage.queue_root", settings.queue_root())?,
        ),
        (
            "storage.repository_root",
            resolve_destination("storage.repository_root", settings.repository_root())?,
        ),
        (
            "storage.content_root",
            resolve_destination("storage.content_root", settings.content_root())?,
        ),
        (
            "storage.work_root",
            resolve_destination("storage.work_root", settings.work_root())?,
        ),
        (
            "storage.release_root",
            resolve_destination("storage.release_root", settings.release_root())?,
        ),
    ];
    for (index, (field, path)) in storage.iter().enumerate() {
        for (other_field, other_path) in &storage[index + 1..] {
            reject_overlap(field, path, other_field, other_path)?;
        }
    }

    let trusted = [
        (
            "quartz.program",
            resolve_destination("quartz.program", settings.quartz_program())?,
        ),
        (
            "quartz.integration_root",
            resolve_destination(
                "quartz.integration_root",
                settings.quartz_integration_root(),
            )?,
        ),
    ];
    for (storage_field, storage_path) in &storage {
        for (trusted_field, trusted_path) in &trusted {
            reject_overlap(storage_field, storage_path, trusted_field, trusted_path)?;
        }
    }
    Ok(())
}

fn resolve_destination(field: &'static str, path: &Path) -> Result<PathBuf, WorkerOpenError> {
    let mut existing = path;
    let mut missing = Vec::<OsString>::new();
    loop {
        match fs::canonicalize(existing) {
            Ok(mut resolved) => {
                for component in missing.iter().rev() {
                    resolved.push(component);
                }
                return Ok(resolved);
            }
            Err(source) if source.kind() == io::ErrorKind::NotFound => {
                let Some(component) = existing.file_name() else {
                    return Err(WorkerOpenError::PathResolution { field, source });
                };
                missing.push(component.to_os_string());
                let Some(parent) = existing.parent() else {
                    return Err(WorkerOpenError::PathResolution { field, source });
                };
                existing = parent;
            }
            Err(source) => return Err(WorkerOpenError::PathResolution { field, source }),
        }
    }
}

fn reject_overlap(
    first: &'static str,
    first_path: &Path,
    second: &'static str,
    second_path: &Path,
) -> Result<(), WorkerOpenError> {
    if first_path.starts_with(second_path) || second_path.starts_with(first_path) {
        return Err(WorkerOpenError::OverlappingPaths { first, second });
    }
    Ok(())
}

fn reject_attested_overlap(
    first: &'static str,
    first_path: &PathAttestation,
    second: &'static str,
    second_path: &PathAttestation,
) -> Result<(), WorkerOpenError> {
    if first_path.is_within(second_path) || second_path.is_within(first_path) {
        return Err(WorkerOpenError::OverlappingPaths { first, second });
    }
    Ok(())
}

#[cfg(test)]
mod tests;

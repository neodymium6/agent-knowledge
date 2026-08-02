use std::fmt;
use std::time::Instant;

use agent_knowledge_core::PathAttestationError;
use agent_knowledge_queue::{QueueError, QueueOverview, QueueReader};
use agent_knowledge_release::{ActiveRelease, ReleaseError, ReleasePolicy, ReleaseReader};
use agent_knowledge_repository::{
    CommittedReadError, CommittedStore, RemoteReplicationError,
    RemoteReplicationStatus as DurableReplicationStatus, read_remote_replication_status,
};
use serde::Serialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::bootstrap::validate_resolved_topology;
use crate::{WorkerOpenError, WorkerSettings};

/// Operational status schema emitted by this release.
pub const CURRENT_OPERATIONAL_STATUS_VERSION: u16 = 1;

/// Read-only status of one configured Worker deployment.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OperationalStatus {
    schema_version: u16,
    observed_at: String,
    queue: QueueStatus,
    publication: PublicationStatus,
    replication: ReplicationStatus,
}

impl OperationalStatus {
    /// Returns the durable queue summary.
    #[must_use]
    pub const fn queue(&self) -> &QueueStatus {
        &self.queue
    }

    /// Returns the local publication summary.
    #[must_use]
    pub const fn publication(&self) -> &PublicationStatus {
        &self.publication
    }

    /// Returns the optional remote replication summary.
    #[must_use]
    pub const fn replication(&self) -> &ReplicationStatus {
        &self.replication
    }
}

/// Counts and liveness observed from the durable request queue.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QueueStatus {
    pending: u64,
    processing: u64,
    completed: u64,
    failed: u64,
    oldest_pending_at: Option<String>,
    worker_active: bool,
}

impl QueueStatus {
    /// Returns the number of requests waiting for the Worker.
    #[must_use]
    pub const fn pending(&self) -> u64 {
        self.pending
    }

    /// Returns the number of requests currently owned by the Worker.
    #[must_use]
    pub const fn processing(&self) -> u64 {
        self.processing
    }

    /// Returns the number of locally published requests.
    #[must_use]
    pub const fn completed(&self) -> u64 {
        self.completed
    }

    /// Returns the number of permanently rejected requests.
    #[must_use]
    pub const fn failed(&self) -> u64 {
        self.failed
    }

    /// Returns the oldest pending acceptance timestamp as RFC 3339 text.
    #[must_use]
    pub fn oldest_pending_at(&self) -> Option<&str> {
        self.oldest_pending_at.as_deref()
    }

    /// Returns whether the Repository Worker lock was held during observation.
    #[must_use]
    pub const fn worker_active(&self) -> bool {
        self.worker_active
    }
}

/// Local Git and static-release publication state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationStatus {
    official_commit: String,
    active_release: Option<ActiveReleaseStatus>,
    synchronized: bool,
}

impl PublicationStatus {
    /// Returns the current official local Git commit.
    #[must_use]
    pub fn official_commit(&self) -> &str {
        &self.official_commit
    }

    /// Returns the selected static release, if publication has occurred.
    #[must_use]
    pub const fn active_release(&self) -> Option<&ActiveReleaseStatus> {
        self.active_release.as_ref()
    }

    /// Returns whether the active release represents the official commit.
    #[must_use]
    pub const fn synchronized(&self) -> bool {
        self.synchronized
    }
}

/// Identity of the static release selected by `releases/current`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ActiveReleaseStatus {
    release_id: String,
    commit: String,
}

impl ActiveReleaseStatus {
    /// Returns the immutable release identifier.
    #[must_use]
    pub fn release_id(&self) -> &str {
        &self.release_id
    }

    /// Returns the Git commit represented by the release.
    #[must_use]
    pub fn commit(&self) -> &str {
        &self.commit
    }
}

/// Remote replication state relative to the observed official commit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ReplicationStatus {
    /// No remote replication policy is configured.
    Disabled,
    /// Replication is configured but has no durable attempt state yet.
    NotAttempted { remote: String, branch: String },
    /// The configured remote has confirmed the observed official commit.
    Current {
        remote: String,
        branch: String,
        commit: String,
    },
    /// The configured remote has not confirmed the observed official commit.
    Lagging {
        remote: String,
        branch: String,
        official_commit: String,
        replicated_commit: Option<String>,
        consecutive_failures: u32,
        retry_at: Option<String>,
    },
}

/// Inspects one initialized Worker deployment without creating, repairing, or
/// publishing any durable state.
///
/// Queue enumeration and Git subprocesses honor `deadline`. Local filesystem
/// calls remain subject to the host filesystem's I/O behavior.
///
/// # Errors
///
/// Returns an error when configured paths overlap or change, a component is
/// absent or corrupt, a bound or deadline is exceeded, or timestamps cannot be
/// encoded as RFC 3339.
pub fn inspect_operational_status(
    settings: &WorkerSettings,
    maximum_queue_entries: usize,
    deadline: Option<Instant>,
    observed_at: OffsetDateTime,
) -> Result<OperationalStatus, OperationalStatusError> {
    let topology =
        validate_resolved_topology(settings).map_err(OperationalStatusError::Topology)?;
    let queue = QueueReader::open_until(topology.queue_root.stable_path().to_path_buf(), deadline)
        .map_err(OperationalStatusError::queue)?;
    let repository = CommittedStore::open_until(
        topology.repository_root.stable_path(),
        topology.content_root.stable_path(),
        settings.official_branch(),
        deadline,
    )
    .map_err(OperationalStatusError::repository)?;
    let releases = ReleaseReader::open(
        topology.release_root.stable_path(),
        ReleasePolicy::default(),
    )
    .map_err(OperationalStatusError::release)?;

    let queue_overview = queue
        .overview_until(maximum_queue_entries, deadline)
        .map_err(OperationalStatusError::queue)?;
    let official_commit = repository
        .current_commit_until(deadline)
        .map_err(OperationalStatusError::repository)?;
    let active_release = releases
        .active_release()
        .map_err(OperationalStatusError::release)?;
    let replication = replication_status(settings, &topology, &official_commit)?;

    queue
        .storage_attestation()
        .map_err(|source| OperationalStatusError::attestation("queue storage", source))?;
    repository
        .storage_attestations()
        .map_err(|source| OperationalStatusError::attestation("repository storage", source))?;
    releases
        .storage_attestation()
        .map_err(|source| OperationalStatusError::attestation("release storage", source))?;

    let queue = queue_status(queue_overview)?;
    let publication = publication_status(official_commit, active_release);
    Ok(OperationalStatus {
        schema_version: CURRENT_OPERATIONAL_STATUS_VERSION,
        observed_at: format_timestamp(observed_at)?,
        queue,
        publication,
        replication,
    })
}

fn queue_status(overview: QueueOverview) -> Result<QueueStatus, OperationalStatusError> {
    Ok(QueueStatus {
        pending: overview.pending(),
        processing: overview.processing(),
        completed: overview.completed(),
        failed: overview.failed(),
        oldest_pending_at: overview
            .oldest_pending_at()
            .map(format_timestamp)
            .transpose()?,
        worker_active: overview.worker_active(),
    })
}

fn publication_status(
    official_commit: String,
    active_release: Option<ActiveRelease>,
) -> PublicationStatus {
    let synchronized = active_release
        .as_ref()
        .is_some_and(|release| release.commit() == official_commit);
    let active_release = active_release.map(|release| ActiveReleaseStatus {
        release_id: release.release_id().to_owned(),
        commit: release.commit().to_owned(),
    });
    PublicationStatus {
        official_commit,
        active_release,
        synchronized,
    }
}

fn replication_status(
    settings: &WorkerSettings,
    topology: &crate::bootstrap::ResolvedTopology,
    official_commit: &str,
) -> Result<ReplicationStatus, OperationalStatusError> {
    let Some(policy) = settings.replication() else {
        return Ok(ReplicationStatus::Disabled);
    };
    let remote = policy.remote().to_owned();
    let branch = policy.branch().to_owned();
    let durable = read_remote_replication_status(topology.repository_root.stable_path(), policy)
        .map_err(OperationalStatusError::replication)?;
    let Some(durable) = durable else {
        return Ok(ReplicationStatus::NotAttempted { remote, branch });
    };
    if durable.replicated_commit() == Some(official_commit) {
        return Ok(ReplicationStatus::Current {
            remote,
            branch,
            commit: official_commit.to_owned(),
        });
    }
    lagging_replication(remote, branch, official_commit, durable)
}

fn lagging_replication(
    remote: String,
    branch: String,
    official_commit: &str,
    durable: DurableReplicationStatus,
) -> Result<ReplicationStatus, OperationalStatusError> {
    Ok(ReplicationStatus::Lagging {
        remote,
        branch,
        official_commit: official_commit.to_owned(),
        replicated_commit: durable.replicated_commit().map(str::to_owned),
        consecutive_failures: durable.consecutive_failures(),
        retry_at: durable.retry_at().map(format_timestamp).transpose()?,
    })
}

fn format_timestamp(timestamp: OffsetDateTime) -> Result<String, OperationalStatusError> {
    timestamp
        .format(&Rfc3339)
        .map_err(OperationalStatusError::Timestamp)
}

/// Failure while collecting a read-only operational snapshot.
#[derive(Debug)]
pub enum OperationalStatusError {
    Topology(WorkerOpenError),
    Attestation {
        component: &'static str,
        source: PathAttestationError,
    },
    Queue(Box<QueueError>),
    Repository(Box<CommittedReadError>),
    Release(Box<ReleaseError>),
    Replication(Box<RemoteReplicationError>),
    Timestamp(time::error::Format),
}

impl OperationalStatusError {
    fn attestation(component: &'static str, source: PathAttestationError) -> Self {
        Self::Attestation { component, source }
    }

    fn queue(error: QueueError) -> Self {
        Self::Queue(Box::new(error))
    }

    fn repository(error: CommittedReadError) -> Self {
        Self::Repository(Box::new(error))
    }

    fn release(error: ReleaseError) -> Self {
        Self::Release(Box::new(error))
    }

    fn replication(error: RemoteReplicationError) -> Self {
        Self::Replication(Box::new(error))
    }
}

impl fmt::Display for OperationalStatusError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Topology(error) => write!(formatter, "invalid Worker topology: {error}"),
            Self::Attestation { component, source } => {
                write!(formatter, "could not attest {component}: {source}")
            }
            Self::Queue(error) => write!(formatter, "could not inspect queue: {error}"),
            Self::Repository(error) => write!(formatter, "could not inspect repository: {error}"),
            Self::Release(error) => write!(formatter, "could not inspect releases: {error}"),
            Self::Replication(error) => {
                write!(formatter, "could not inspect remote replication: {error}")
            }
            Self::Timestamp(error) => {
                write!(formatter, "could not format status timestamp: {error}")
            }
        }
    }
}

impl std::error::Error for OperationalStatusError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Topology(error) => Some(error),
            Self::Attestation { source, .. } => Some(source),
            Self::Queue(error) => Some(error),
            Self::Repository(error) => Some(error),
            Self::Release(error) => Some(error),
            Self::Replication(error) => Some(error),
            Self::Timestamp(error) => Some(error),
        }
    }
}

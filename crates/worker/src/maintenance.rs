use std::fmt;
use std::time::Instant;

use agent_knowledge_release::{ReleaseError, ReleasePolicy, ReleaseRetentionOutcome, ReleaseStore};

use crate::bootstrap::{WorkerOpenError, validate_resolved_topology};
use crate::config::WorkerSettings;

/// Applies one bounded retention pass to derived Quartz releases.
///
/// The operation validates all resolved storage boundaries before opening the
/// configured release store. It never creates missing storage.
///
/// # Errors
///
/// Returns an error for unsafe topology, an absent or corrupt release store,
/// lock contention, exhausted bounds, deadline expiry, or cleanup failures.
pub fn retain_derived_releases(
    settings: &WorkerSettings,
    dry_run: bool,
    deadline: Option<Instant>,
) -> Result<ReleaseRetentionOutcome, ReleaseMaintenanceError> {
    let topology =
        validate_resolved_topology(settings).map_err(ReleaseMaintenanceError::Topology)?;
    let releases = ReleaseStore::open_existing(
        topology.release_root.stable_path(),
        ReleasePolicy::default(),
    )
    .map_err(ReleaseMaintenanceError::Release)?;
    releases
        .retain_releases_until(settings.release_retention(), dry_run, deadline)
        .map_err(ReleaseMaintenanceError::Release)
}

/// Failure while applying local derived-release retention.
#[derive(Debug)]
pub enum ReleaseMaintenanceError {
    /// Configured paths resolve to an unsafe or overlapping topology.
    Topology(WorkerOpenError),
    /// The release store could not be opened or retained safely.
    Release(ReleaseError),
}

impl fmt::Display for ReleaseMaintenanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Topology(error) => write!(formatter, "unsafe Worker topology: {error}"),
            Self::Release(error) => write!(formatter, "release retention failed: {error}"),
        }
    }
}

impl std::error::Error for ReleaseMaintenanceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Topology(error) => Some(error),
            Self::Release(error) => Some(error),
        }
    }
}

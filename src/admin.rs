use std::fmt;
use std::io::{self, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use agent_knowledge_worker::{
    OperationalStatusError, ReleaseMaintenanceError, WorkerConfigError, WorkerSettings,
    inspect_operational_status, retain_derived_releases,
};
use time::OffsetDateTime;

pub(crate) fn status<W>(
    config: &Path,
    maximum_queue_entries: usize,
    timeout: Duration,
    mut output: W,
) -> Result<(), AdminStatusError>
where
    W: Write,
{
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or(AdminStatusError::DeadlineOverflow)?;
    let settings = WorkerSettings::load(config).map_err(AdminStatusError::Config)?;
    let status = inspect_operational_status(
        &settings,
        maximum_queue_entries,
        Some(deadline),
        OffsetDateTime::now_utc(),
    )
    .map_err(AdminStatusError::Inspect)?;
    serde_json::to_writer(&mut output, &status).map_err(AdminStatusError::Json)?;
    output.write_all(b"\n").map_err(AdminStatusError::Io)?;
    output.flush().map_err(AdminStatusError::Io)
}

pub(crate) fn prune_releases<W>(
    config: &Path,
    dry_run: bool,
    timeout: Duration,
    mut output: W,
) -> Result<(), AdminRetentionError>
where
    W: Write,
{
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or(AdminRetentionError::DeadlineOverflow)?;
    let settings = WorkerSettings::load(config).map_err(AdminRetentionError::Config)?;
    let outcome = retain_derived_releases(&settings, dry_run, Some(deadline))
        .map_err(AdminRetentionError::Retain)?;
    serde_json::to_writer(&mut output, &outcome).map_err(AdminRetentionError::Json)?;
    output.write_all(b"\n").map_err(AdminRetentionError::Io)?;
    output.flush().map_err(AdminRetentionError::Io)
}

#[derive(Debug)]
pub(crate) enum AdminStatusError {
    Config(WorkerConfigError),
    Inspect(OperationalStatusError),
    DeadlineOverflow,
    Json(serde_json::Error),
    Io(io::Error),
}

impl fmt::Display for AdminStatusError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(error) => write!(formatter, "invalid Worker configuration: {error}"),
            Self::Inspect(error) => write!(formatter, "operational status failed: {error}"),
            Self::DeadlineOverflow => formatter.write_str("operational status deadline overflowed"),
            Self::Json(error) => write!(formatter, "operational status JSON failed: {error}"),
            Self::Io(error) => write!(formatter, "operational status output failed: {error}"),
        }
    }
}

impl std::error::Error for AdminStatusError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Config(error) => Some(error),
            Self::Inspect(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::Io(error) => Some(error),
            Self::DeadlineOverflow => None,
        }
    }
}

#[derive(Debug)]
pub(crate) enum AdminRetentionError {
    Config(WorkerConfigError),
    Retain(ReleaseMaintenanceError),
    DeadlineOverflow,
    Json(serde_json::Error),
    Io(io::Error),
}

impl fmt::Display for AdminRetentionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(error) => write!(formatter, "invalid Worker configuration: {error}"),
            Self::Retain(error) => write!(formatter, "release maintenance failed: {error}"),
            Self::DeadlineOverflow => {
                formatter.write_str("release maintenance deadline overflowed")
            }
            Self::Json(error) => write!(formatter, "release maintenance JSON failed: {error}"),
            Self::Io(error) => write!(formatter, "release maintenance output failed: {error}"),
        }
    }
}

impl std::error::Error for AdminRetentionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Config(error) => Some(error),
            Self::Retain(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::Io(error) => Some(error),
            Self::DeadlineOverflow => None,
        }
    }
}

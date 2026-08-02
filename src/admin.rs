use std::fmt;
use std::io::{self, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use agent_knowledge_worker::{
    OperationalStatusError, WorkerConfigError, WorkerSettings, inspect_operational_status,
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

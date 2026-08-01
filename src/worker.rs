use std::fmt;
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration as StandardDuration, Instant};

use agent_knowledge_worker::{
    BatchCloseReason, StartupOutcome, WorkerBootstrap, WorkerConfigError, WorkerOpenError,
    WorkerPollOutcome, WorkerRunError, WorkerSettings,
};
use serde::Serialize;
use signal_hook::consts::{SIGINT, SIGTERM};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

const MAXIMUM_SIGNAL_WAIT: StandardDuration = StandardDuration::from_millis(250);

pub(crate) fn run<W>(config: &Path, mut output: W) -> Result<(), WorkerCommandError>
where
    W: Write,
{
    let stopping = Arc::new(AtomicBool::new(false));
    let _sigint = signal_hook::flag::register(SIGINT, Arc::clone(&stopping))
        .map_err(WorkerCommandError::SignalRegistration)?;
    let _sigterm = signal_hook::flag::register(SIGTERM, Arc::clone(&stopping))
        .map_err(WorkerCommandError::SignalRegistration)?;

    let settings = WorkerSettings::load(config).map_err(WorkerCommandError::Config)?;
    let bootstrap = WorkerBootstrap::open(settings).map_err(WorkerCommandError::Open)?;
    if stopping.load(Ordering::Relaxed) {
        return Ok(());
    }
    let started_at = OffsetDateTime::now_utc();
    let (mut runtime, startup, schedule) = bootstrap
        .start(started_at)
        .map_err(WorkerCommandError::Run)?;
    write_startup_log(&mut output, started_at, &startup)?;

    while !stopping.load(Ordering::Relaxed) {
        let now = OffsetDateTime::now_utc();
        let outcome = runtime
            .poll_once(schedule, now)
            .map_err(WorkerCommandError::Run)?;
        write_poll_log(&mut output, now, &outcome)?;
        if let Some(duration) = wait_after_poll(&outcome, now) {
            wait_for_termination(&stopping, duration);
        }
    }
    write_worker_log(
        &mut output,
        WorkerLogRecord::new(OffsetDateTime::now_utc(), "worker_stopped"),
    )
}

fn wait_after_poll(outcome: &WorkerPollOutcome, now: OffsetDateTime) -> Option<StandardDuration> {
    match outcome {
        WorkerPollOutcome::Idle => Some(MAXIMUM_SIGNAL_WAIT),
        WorkerPollOutcome::Waiting { ready_at } => {
            let remaining = *ready_at - now;
            if remaining.is_positive() {
                Some(StandardDuration::try_from(remaining).unwrap_or(MAXIMUM_SIGNAL_WAIT))
            } else {
                None
            }
        }
        WorkerPollOutcome::Scanning { .. }
        | WorkerPollOutcome::ClosedWithoutCommit { .. }
        | WorkerPollOutcome::Processed { .. } => None,
    }
}

fn wait_for_termination(stopping: &AtomicBool, duration: StandardDuration) {
    let started = Instant::now();
    while !stopping.load(Ordering::Relaxed) {
        let remaining = duration.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return;
        }
        thread::sleep(remaining.min(MAXIMUM_SIGNAL_WAIT));
    }
}

fn write_startup_log<W>(
    output: &mut W,
    timestamp: OffsetDateTime,
    startup: &StartupOutcome,
) -> Result<(), WorkerCommandError>
where
    W: Write,
{
    let record = match startup {
        StartupOutcome::Clean => WorkerLogRecord::new(timestamp, "worker_started"),
        StartupOutcome::Requeued { batch_id, requests } => {
            let mut record = WorkerLogRecord::new(timestamp, "worker_requeued_claims");
            record.batch_id = Some(batch_id.to_string());
            record.requests = Some(*requests);
            record
        }
        StartupOutcome::Resumed { batch_id, .. } => {
            let mut record = WorkerLogRecord::new(timestamp, "worker_resumed_batch");
            record.batch_id = Some(batch_id.to_string());
            record
        }
    };
    write_worker_log(output, record)
}

fn write_poll_log<W>(
    output: &mut W,
    timestamp: OffsetDateTime,
    outcome: &WorkerPollOutcome,
) -> Result<(), WorkerCommandError>
where
    W: Write,
{
    let record = match outcome {
        WorkerPollOutcome::ClosedWithoutCommit { reason } => {
            let mut record = WorkerLogRecord::new(timestamp, "batch_closed_without_commit");
            record.close_reason = Some(close_reason_name(*reason));
            Some(record)
        }
        WorkerPollOutcome::Processed {
            reason, batch_id, ..
        } => {
            let mut record = WorkerLogRecord::new(timestamp, "batch_processed");
            record.batch_id = Some(batch_id.to_string());
            record.close_reason = Some(close_reason_name(*reason));
            Some(record)
        }
        WorkerPollOutcome::Scanning { .. }
        | WorkerPollOutcome::Idle
        | WorkerPollOutcome::Waiting { .. } => None,
    };
    match record {
        Some(record) => write_worker_log(output, record),
        None => Ok(()),
    }
}

fn close_reason_name(reason: BatchCloseReason) -> &'static str {
    match reason {
        BatchCloseReason::InvalidAcceptance => "invalid_acceptance",
        BatchCloseReason::MaximumRequests => "maximum_requests",
        BatchCloseReason::MaximumAge => "maximum_age",
        BatchCloseReason::Debounce => "debounce",
    }
}

fn write_worker_log<W>(
    output: &mut W,
    mut record: WorkerLogRecord,
) -> Result<(), WorkerCommandError>
where
    W: Write,
{
    record.timestamp = record
        .raw_timestamp
        .format(&Rfc3339)
        .map_err(WorkerCommandError::Timestamp)?;
    serde_json::to_writer(&mut *output, &record).map_err(WorkerCommandError::Json)?;
    output.write_all(b"\n").map_err(WorkerCommandError::Io)?;
    output.flush().map_err(WorkerCommandError::Io)
}

#[derive(Serialize)]
struct WorkerLogRecord {
    timestamp: String,
    #[serde(skip)]
    raw_timestamp: OffsetDateTime,
    severity: &'static str,
    component: &'static str,
    event: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    batch_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    requests: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    close_reason: Option<&'static str>,
}

impl WorkerLogRecord {
    fn new(raw_timestamp: OffsetDateTime, event: &'static str) -> Self {
        Self {
            timestamp: String::new(),
            raw_timestamp,
            severity: "info",
            component: "worker",
            event,
            batch_id: None,
            requests: None,
            close_reason: None,
        }
    }
}

#[derive(Debug)]
pub(crate) enum WorkerCommandError {
    Config(WorkerConfigError),
    Open(WorkerOpenError),
    Run(WorkerRunError),
    SignalRegistration(io::Error),
    Timestamp(time::error::Format),
    Json(serde_json::Error),
    Io(io::Error),
}

impl fmt::Display for WorkerCommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(error) => write!(formatter, "invalid Worker configuration: {error}"),
            Self::Open(error) => write!(formatter, "Worker startup failed: {error}"),
            Self::Run(error) => write!(formatter, "Worker cycle failed: {error}"),
            Self::SignalRegistration(error) => {
                write!(
                    formatter,
                    "could not install Worker signal handlers: {error}"
                )
            }
            Self::Timestamp(error) => write!(formatter, "could not format log timestamp: {error}"),
            Self::Json(error) => write!(formatter, "JSON log encoding failed: {error}"),
            Self::Io(error) => write!(formatter, "Worker log output failed: {error}"),
        }
    }
}

impl std::error::Error for WorkerCommandError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Config(error) => Some(error),
            Self::Open(error) => Some(error),
            Self::Run(error) => Some(error),
            Self::SignalRegistration(error) => Some(error),
            Self::Timestamp(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::Io(error) => Some(error),
        }
    }
}

#[cfg(test)]
mod tests;

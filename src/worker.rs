use std::fmt;
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration as StandardDuration, Instant};

use agent_knowledge_worker::{
    BatchCloseReason, BatchCommitOutcome, InterruptibleStart, RemoteReplicationOutcome,
    ReplicationEventError, StartupOutcome, WorkerBootstrap, WorkerConfigError, WorkerOpenError,
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
    match run_inner(config, &mut output) {
        Ok(()) => Ok(()),
        Err(error) => match write_failure_log(&mut output, &error) {
            Ok(()) => Err(error),
            Err(reporting) => Err(WorkerCommandError::FailureReporting {
                operation: Box::new(error),
                reporting: Box::new(reporting),
            }),
        },
    }
}

fn write_failure_log<W>(
    output: &mut W,
    error: &WorkerCommandError,
) -> Result<(), WorkerCommandError>
where
    W: Write,
{
    let Some(error_code) = error.stable_code() else {
        return Ok(());
    };
    let mut record = WorkerLogRecord::new(OffsetDateTime::now_utc(), "worker_failed");
    record.severity = "error";
    record.error_code = Some(error_code);
    let failed_requests = error.failed_requests();
    if failed_requests > 0 {
        record.failed_requests = Some(failed_requests);
    }
    write_worker_log(output, record)
}

fn run_inner<W>(config: &Path, output: &mut W) -> Result<(), WorkerCommandError>
where
    W: Write,
{
    let stopping = Arc::new(AtomicBool::new(false));
    let _sigint = signal_hook::flag::register(SIGINT, Arc::clone(&stopping))
        .map_err(WorkerCommandError::SignalRegistration)?;
    let _sigterm = signal_hook::flag::register(SIGTERM, Arc::clone(&stopping))
        .map_err(WorkerCommandError::SignalRegistration)?;

    let should_stop = || stopping.load(Ordering::Relaxed);
    let settings = WorkerSettings::load(config).map_err(WorkerCommandError::Config)?;
    let bootstrap = WorkerBootstrap::open(settings).map_err(WorkerCommandError::Open)?;
    if should_stop() {
        return write_stopped_log(output, 0);
    }
    let started_at = OffsetDateTime::now_utc();
    let started = bootstrap
        .start_interruptible(started_at, &should_stop)
        .map_err(WorkerCommandError::Run)?;
    let (mut runtime, startup, schedule) = match started {
        InterruptibleStart::Started(started) => started,
        InterruptibleStart::Stopped { failed_requests } => {
            return write_stopped_log(output, failed_requests);
        }
    };
    write_startup_log(output, OffsetDateTime::now_utc(), &startup)?;

    let mut stopped_failures = 0;
    while !should_stop() {
        let operation_at = OffsetDateTime::now_utc();
        let outcome = runtime
            .poll_once_interruptible(schedule, operation_at, &should_stop)
            .map_err(WorkerCommandError::Run)?;
        let completed_at = OffsetDateTime::now_utc();
        if let WorkerPollOutcome::Stopped { failed_requests } = outcome {
            stopped_failures = failed_requests;
            break;
        }
        write_poll_log(output, completed_at, &outcome)?;
        if let Some(replication) = runtime.take_replication_event() {
            match replication {
                Ok(outcome) => write_replication_log(output, completed_at, Some(&outcome))?,
                Err(error) => write_replication_error_log(output, completed_at, &error)?,
            }
        }
        if let Some(duration) = wait_after_poll(&outcome, completed_at) {
            let _ = wait_for_termination(&stopping, duration);
        }
    }
    write_stopped_log(output, stopped_failures)
}

fn write_replication_log<W>(
    output: &mut W,
    timestamp: OffsetDateTime,
    outcome: Option<&RemoteReplicationOutcome>,
) -> Result<(), WorkerCommandError>
where
    W: Write,
{
    let record = match outcome {
        Some(RemoteReplicationOutcome::Pushed { commit }) => {
            let mut record = WorkerLogRecord::new(timestamp, "remote_replication_succeeded");
            record.commit = Some(commit.clone());
            Some(record)
        }
        Some(RemoteReplicationOutcome::Failed {
            commit,
            consecutive_failures,
            retry_at,
        }) => {
            let mut record = WorkerLogRecord::new(timestamp, "remote_replication_failed");
            record.severity = "warning";
            record.commit = Some(commit.clone());
            record.consecutive_failures = Some(*consecutive_failures);
            record.retry_at = Some(
                retry_at
                    .format(&Rfc3339)
                    .map_err(WorkerCommandError::Timestamp)?,
            );
            Some(record)
        }
        None
        | Some(RemoteReplicationOutcome::Cancelled)
        | Some(RemoteReplicationOutcome::UpToDate { .. })
        | Some(RemoteReplicationOutcome::Deferred { .. }) => None,
    };
    match record {
        Some(record) => write_worker_log(output, record),
        None => Ok(()),
    }
}

fn write_replication_error_log<W>(
    output: &mut W,
    timestamp: OffsetDateTime,
    error: &ReplicationEventError,
) -> Result<(), WorkerCommandError>
where
    W: Write,
{
    let event = match error {
        ReplicationEventError::Attempt(_) => "remote_replication_state_error",
        ReplicationEventError::ThreadStopped => "remote_replication_thread_stopped",
    };
    let mut record = WorkerLogRecord::new(timestamp, event);
    record.severity = "error";
    record.error_code = Some(event);
    write_worker_log(output, record)
}

fn write_stopped_log<W>(output: &mut W, failed_requests: usize) -> Result<(), WorkerCommandError>
where
    W: Write,
{
    let mut record = WorkerLogRecord::new(OffsetDateTime::now_utc(), "worker_stopped");
    if failed_requests > 0 {
        record.severity = "warning";
        record.failed_requests = Some(failed_requests);
    }
    write_worker_log(output, record)
}

fn wait_after_poll(outcome: &WorkerPollOutcome, now: OffsetDateTime) -> Option<StandardDuration> {
    match outcome {
        WorkerPollOutcome::Idle => Some(MAXIMUM_SIGNAL_WAIT),
        WorkerPollOutcome::Waiting { ready_at } => {
            let remaining = *ready_at - now;
            if remaining.is_positive() {
                Some(
                    StandardDuration::try_from(remaining)
                        .unwrap_or(MAXIMUM_SIGNAL_WAIT)
                        .min(MAXIMUM_SIGNAL_WAIT),
                )
            } else {
                None
            }
        }
        WorkerPollOutcome::Stopped { .. }
        | WorkerPollOutcome::Scanning { .. }
        | WorkerPollOutcome::ClosedWithoutCommit { .. }
        | WorkerPollOutcome::Processed { .. } => None,
    }
}

fn wait_for_termination(stopping: &AtomicBool, duration: StandardDuration) -> bool {
    let started = Instant::now();
    while !stopping.load(Ordering::Relaxed) {
        let remaining = duration.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return false;
        }
        thread::sleep(remaining.min(MAXIMUM_SIGNAL_WAIT));
    }
    true
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
        StartupOutcome::Resumed {
            batch_id,
            outcome,
            claim_failures,
        } => {
            let mut record = WorkerLogRecord::new(timestamp, "worker_resumed_batch");
            record.batch_id = Some(batch_id.to_string());
            add_commit_outcome(&mut record, outcome, *claim_failures);
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
        WorkerPollOutcome::ClosedWithoutCommit {
            reason,
            failed_requests,
        } => {
            let mut record = WorkerLogRecord::new(timestamp, "batch_closed_without_commit");
            record.severity = "warning";
            record.close_reason = Some(close_reason_name(*reason));
            record.outcome = Some("no_claimable_requests");
            record.successful_requests = Some(0);
            record.failed_requests = Some(*failed_requests);
            Some(record)
        }
        WorkerPollOutcome::Processed {
            reason,
            batch_id,
            outcome,
            claim_failures,
        } => {
            let mut record = WorkerLogRecord::new(timestamp, "batch_processed");
            record.batch_id = Some(batch_id.to_string());
            record.close_reason = Some(close_reason_name(*reason));
            add_commit_outcome(&mut record, outcome, *claim_failures);
            Some(record)
        }
        WorkerPollOutcome::Stopped { .. }
        | WorkerPollOutcome::Scanning { .. }
        | WorkerPollOutcome::Idle
        | WorkerPollOutcome::Waiting { .. } => None,
    };
    match record {
        Some(record) => write_worker_log(output, record),
        None => Ok(()),
    }
}

fn add_commit_outcome(
    record: &mut WorkerLogRecord,
    outcome: &BatchCommitOutcome,
    claim_failures: usize,
) {
    match outcome {
        BatchCommitOutcome::NoChanges { failures } => {
            record.severity = "warning";
            record.outcome = Some("no_changes");
            record.successful_requests = Some(0);
            record.failed_requests = Some(claim_failures.saturating_add(failures.len()));
        }
        BatchCommitOutcome::Committed {
            commit,
            successful,
            failures,
        } => {
            if claim_failures > 0 || !failures.is_empty() {
                record.severity = "warning";
            }
            record.outcome = Some("committed");
            record.commit = Some(commit.clone());
            record.successful_requests = Some(successful.len());
            record.failed_requests = Some(claim_failures.saturating_add(failures.len()));
        }
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
    #[serde(skip_serializing_if = "Option::is_none")]
    outcome: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    successful_requests: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    failed_requests: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    consecutive_failures: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_code: Option<&'static str>,
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
            outcome: None,
            commit: None,
            successful_requests: None,
            failed_requests: None,
            consecutive_failures: None,
            retry_at: None,
            error_code: None,
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
    FailureReporting {
        operation: Box<WorkerCommandError>,
        reporting: Box<WorkerCommandError>,
    },
}

impl WorkerCommandError {
    fn stable_code(&self) -> Option<&'static str> {
        match self {
            Self::Config(_) => Some("worker_config_invalid"),
            Self::Open(_) => Some("worker_startup_failed"),
            Self::Run(_) => Some("worker_cycle_failed"),
            Self::SignalRegistration(_) => Some("worker_signal_registration_failed"),
            Self::Timestamp(_) | Self::Json(_) | Self::Io(_) | Self::FailureReporting { .. } => {
                None
            }
        }
    }

    fn failed_requests(&self) -> usize {
        match self {
            Self::Run(error) => error.failed_requests(),
            Self::FailureReporting { operation, .. } => operation.failed_requests(),
            _ => 0,
        }
    }
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
            Self::FailureReporting {
                operation,
                reporting,
            } => write!(
                formatter,
                "{operation}; structured failure reporting also failed: {reporting}"
            ),
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
            Self::FailureReporting { operation, .. } => Some(operation),
        }
    }
}

#[cfg(test)]
mod tests;

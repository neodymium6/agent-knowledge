use std::sync::atomic::AtomicBool;
use std::time::Duration as StandardDuration;

use agent_knowledge_queue::WorkerQueueError;
use agent_knowledge_worker::{
    BatchCommitOutcome, StartupOutcome, WorkerPollOutcome, WorkerRunError, WorkerSettings,
};
use time::{Duration, OffsetDateTime};

use super::{
    MAXIMUM_SIGNAL_WAIT, WorkerCommandError, WorkerLogRecord, add_commit_outcome, wait_after_poll,
    wait_for_termination, write_failure_log, write_poll_log, write_startup_log, write_stopped_log,
    write_worker_log,
};

#[test]
fn waits_until_the_batch_deadline_or_idle_poll() {
    let now = OffsetDateTime::parse(
        "2026-08-01T04:00:00Z",
        &time::format_description::well_known::Rfc3339,
    )
    .unwrap_or_else(|error| panic!("fixture timestamp must parse: {error}"));
    let waiting = WorkerPollOutcome::Waiting {
        ready_at: now + Duration::seconds(30),
    };

    assert_eq!(wait_after_poll(&waiting, now), Some(MAXIMUM_SIGNAL_WAIT));
    assert_eq!(
        wait_after_poll(&WorkerPollOutcome::Idle, now),
        Some(MAXIMUM_SIGNAL_WAIT)
    );
}

#[test]
fn returns_immediately_when_termination_was_already_requested() {
    let stopping = AtomicBool::new(true);

    assert!(wait_for_termination(
        &stopping,
        StandardDuration::from_secs(60)
    ));
}

#[test]
fn reports_when_wait_deadline_expires_without_termination() {
    let stopping = AtomicBool::new(false);

    assert!(!wait_for_termination(&stopping, StandardDuration::ZERO));
}

#[test]
fn startup_log_is_structured_json() {
    let timestamp = OffsetDateTime::parse(
        "2026-08-01T04:00:00Z",
        &time::format_description::well_known::Rfc3339,
    )
    .unwrap_or_else(|error| panic!("fixture timestamp must parse: {error}"));
    let mut output = Vec::new();
    write_startup_log(&mut output, timestamp, &StartupOutcome::Clean)
        .unwrap_or_else(|error| panic!("startup log must serialize: {error}"));
    let value: serde_json::Value = serde_json::from_slice(&output)
        .unwrap_or_else(|error| panic!("startup log must be JSON: {error}"));

    assert_eq!(value["timestamp"], "2026-08-01T04:00:00Z");
    assert_eq!(value["severity"], "info");
    assert_eq!(value["component"], "worker");
    assert_eq!(value["event"], "worker_started");
    assert!(value.get("batch_id").is_none());
}

#[test]
fn commit_outcomes_have_stable_structured_fields() {
    let timestamp = OffsetDateTime::parse(
        "2026-08-01T04:00:00Z",
        &time::format_description::well_known::Rfc3339,
    )
    .unwrap_or_else(|error| panic!("fixture timestamp must parse: {error}"));
    let cases = [
        BatchCommitOutcome::NoChanges {
            failures: Vec::new(),
        },
        BatchCommitOutcome::Committed {
            commit: "0123456789abcdef".into(),
            successful: Vec::new(),
            failures: Vec::new(),
        },
    ];

    for outcome in cases {
        let mut record = WorkerLogRecord::new(timestamp, "batch_processed");
        add_commit_outcome(&mut record, &outcome, 0);
        let mut output = Vec::new();
        write_worker_log(&mut output, record)
            .unwrap_or_else(|error| panic!("outcome log must serialize: {error}"));
        let value: serde_json::Value = serde_json::from_slice(&output)
            .unwrap_or_else(|error| panic!("outcome log must be JSON: {error}"));

        assert!(matches!(
            value["outcome"].as_str(),
            Some("no_changes" | "committed")
        ));
        assert_eq!(value["successful_requests"], 0);
        assert_eq!(value["failed_requests"], 0);
        if value["outcome"] == "committed" {
            assert_eq!(value["commit"], "0123456789abcdef");
        } else {
            assert!(value.get("commit").is_none());
        }
    }
}

#[test]
fn terminal_batch_events_report_request_outcomes() {
    let timestamp = OffsetDateTime::parse(
        "2026-08-01T04:00:00Z",
        &time::format_description::well_known::Rfc3339,
    )
    .unwrap_or_else(|error| panic!("fixture timestamp must parse: {error}"));
    let mut output = Vec::new();
    let batch_id = "01K00000000000000000000003"
        .parse()
        .unwrap_or_else(|error| panic!("fixture batch ID must parse: {error}"));
    write_poll_log(
        &mut output,
        timestamp,
        &WorkerPollOutcome::Processed {
            reason: agent_knowledge_worker::BatchCloseReason::MaximumRequests,
            batch_id,
            outcome: BatchCommitOutcome::Committed {
                commit: "0123456789abcdef".into(),
                successful: Vec::new(),
                failures: Vec::new(),
            },
            claim_failures: 2,
        },
    )
    .unwrap_or_else(|error| panic!("processed event must serialize: {error}"));
    let processed: serde_json::Value = serde_json::from_slice(&output)
        .unwrap_or_else(|error| panic!("processed event must be JSON: {error}"));
    assert_eq!(processed["event"], "batch_processed");
    assert_eq!(processed["severity"], "warning");
    assert_eq!(processed["outcome"], "committed");
    assert_eq!(processed["successful_requests"], 0);
    assert_eq!(processed["failed_requests"], 2);

    output.clear();
    write_poll_log(
        &mut output,
        timestamp,
        &WorkerPollOutcome::ClosedWithoutCommit {
            reason: agent_knowledge_worker::BatchCloseReason::InvalidAcceptance,
            failed_requests: 3,
        },
    )
    .unwrap_or_else(|error| panic!("closed event must serialize: {error}"));
    let closed: serde_json::Value = serde_json::from_slice(&output)
        .unwrap_or_else(|error| panic!("closed event must be JSON: {error}"));
    assert_eq!(closed["event"], "batch_closed_without_commit");
    assert_eq!(closed["outcome"], "no_claimable_requests");
    assert_eq!(closed["successful_requests"], 0);
    assert_eq!(closed["failed_requests"], 3);
}

#[test]
fn configuration_failures_emit_a_structured_terminal_event() {
    let config_error = match WorkerSettings::decode("") {
        Ok(_) => panic!("an empty Worker configuration must be rejected"),
        Err(error) => error,
    };
    let error = WorkerCommandError::Config(config_error);
    let mut output = Vec::new();
    let result = write_failure_log(&mut output, &error);

    assert!(result.is_ok());
    let value: serde_json::Value = serde_json::from_slice(&output)
        .unwrap_or_else(|error| panic!("failure log must be JSON: {error}"));
    assert_eq!(value["severity"], "error");
    assert_eq!(value["component"], "worker");
    assert_eq!(value["event"], "worker_failed");
    assert_eq!(value["error_code"], "worker_config_invalid");
}

#[test]
fn cycle_failures_report_requests_rejected_before_the_error() {
    let error = WorkerCommandError::Run(WorkerRunError::Queue(Box::new(
        WorkerQueueError::BatchScanFailed {
            failed_requests: 3,
            source: Box::new(WorkerQueueError::InvalidBatchLimits),
        },
    )));
    let mut output = Vec::new();

    write_failure_log(&mut output, &error)
        .unwrap_or_else(|reporting| panic!("failure log must serialize: {reporting}"));
    let value: serde_json::Value = serde_json::from_slice(&output)
        .unwrap_or_else(|decode| panic!("failure log must be JSON: {decode}"));

    assert_eq!(value["event"], "worker_failed");
    assert_eq!(value["failed_requests"], 3);
}

#[test]
fn recovery_failures_and_stops_report_journaled_request_failures() {
    let batch_id = "01K00000000000000000000003"
        .parse()
        .unwrap_or_else(|error| panic!("fixture batch ID must parse: {error}"));
    let error = WorkerCommandError::Run(WorkerRunError::MissingProcessingClaims {
        batch_id,
        failed_requests: 4,
    });
    let mut output = Vec::new();
    write_failure_log(&mut output, &error)
        .unwrap_or_else(|reporting| panic!("failure log must serialize: {reporting}"));
    let failed: serde_json::Value = serde_json::from_slice(&output)
        .unwrap_or_else(|decode| panic!("failure log must be JSON: {decode}"));
    assert_eq!(failed["failed_requests"], 4);

    output.clear();
    write_stopped_log(&mut output, 5)
        .unwrap_or_else(|reporting| panic!("stop log must serialize: {reporting}"));
    let stopped: serde_json::Value = serde_json::from_slice(&output)
        .unwrap_or_else(|decode| panic!("stop log must be JSON: {decode}"));
    assert_eq!(stopped["event"], "worker_stopped");
    assert_eq!(stopped["severity"], "warning");
    assert_eq!(stopped["failed_requests"], 5);
}

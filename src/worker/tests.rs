use std::sync::atomic::AtomicBool;
use std::time::{Duration as StandardDuration, Instant};

use agent_knowledge_worker::{StartupOutcome, WorkerPollOutcome};
use time::{Duration, OffsetDateTime};

use super::{MAXIMUM_SIGNAL_WAIT, wait_after_poll, wait_for_termination, write_startup_log};

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

    assert_eq!(
        wait_after_poll(&waiting, now),
        Some(StandardDuration::from_secs(30))
    );
    assert_eq!(
        wait_after_poll(&WorkerPollOutcome::Idle, now),
        Some(MAXIMUM_SIGNAL_WAIT)
    );
}

#[test]
fn returns_immediately_when_termination_was_already_requested() {
    let stopping = AtomicBool::new(true);
    let started = Instant::now();

    wait_for_termination(&stopping, StandardDuration::from_secs(60));

    assert!(started.elapsed() < MAXIMUM_SIGNAL_WAIT);
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

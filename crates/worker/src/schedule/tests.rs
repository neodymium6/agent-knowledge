use std::num::NonZeroUsize;

use time::{Duration, OffsetDateTime};

use super::{BatchCloseReason, BatchReadiness, BatchSchedule, BatchScheduleError, readiness_for};

fn timestamp(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(seconds)
        .unwrap_or_else(|error| panic!("fixture timestamp must be valid: {error}"))
}

fn maximum_requests() -> NonZeroUsize {
    NonZeroUsize::new(100).unwrap_or(NonZeroUsize::MIN)
}

fn schedule() -> BatchSchedule {
    BatchSchedule::new(Duration::seconds(30), Duration::minutes(5))
        .unwrap_or_else(|error| panic!("fixture schedule must be valid: {error}"))
}

#[test]
fn rejects_nonpositive_time_thresholds() {
    assert_eq!(
        BatchSchedule::new(Duration::ZERO, Duration::minutes(5)),
        Err(BatchScheduleError::InvalidDebounce)
    );
    assert_eq!(
        BatchSchedule::new(Duration::seconds(30), Duration::seconds(-1)),
        Err(BatchScheduleError::InvalidMaximumAge)
    );
}

#[test]
fn empty_and_invalid_snapshots_do_not_wait() {
    assert_eq!(
        readiness_for(
            0,
            false,
            None,
            None,
            maximum_requests(),
            schedule(),
            timestamp(0),
        ),
        BatchReadiness::Empty
    );
    assert_eq!(
        readiness_for(
            1,
            true,
            None,
            None,
            maximum_requests(),
            schedule(),
            timestamp(0),
        ),
        BatchReadiness::Ready {
            reason: BatchCloseReason::InvalidAcceptance,
        }
    );
}

#[test]
fn closes_on_count_age_and_inactivity_thresholds() {
    let schedule = schedule();
    let oldest = timestamp(1_000);
    let newest = timestamp(1_100);
    assert_eq!(
        readiness_for(
            100,
            false,
            Some(oldest),
            Some(newest),
            maximum_requests(),
            schedule,
            newest,
        ),
        BatchReadiness::Ready {
            reason: BatchCloseReason::MaximumRequests,
        }
    );
    assert_eq!(
        readiness_for(
            2,
            false,
            Some(oldest),
            Some(newest),
            maximum_requests(),
            schedule,
            oldest + Duration::minutes(5),
        ),
        BatchReadiness::Ready {
            reason: BatchCloseReason::MaximumAge,
        }
    );
    assert_eq!(
        readiness_for(
            2,
            false,
            Some(oldest),
            Some(newest),
            maximum_requests(),
            schedule,
            newest + Duration::seconds(30),
        ),
        BatchReadiness::Ready {
            reason: BatchCloseReason::Debounce,
        }
    );
}

#[test]
fn waits_until_the_earlier_time_threshold() {
    let oldest = timestamp(1_000);
    let newest = timestamp(1_280);
    assert_eq!(
        readiness_for(
            2,
            false,
            Some(oldest),
            Some(newest),
            maximum_requests(),
            schedule(),
            timestamp(1_290),
        ),
        BatchReadiness::Waiting {
            ready_at: timestamp(1_300),
        }
    );
}

use std::fmt;
use std::num::NonZeroUsize;

use agent_knowledge_queue::PendingSnapshot;
use time::{Duration, OffsetDateTime};

/// Time thresholds that decide when accumulated pending work closes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BatchSchedule {
    debounce: Duration,
    maximum_age: Duration,
}

impl BatchSchedule {
    /// Creates a schedule from strictly positive time thresholds.
    ///
    /// # Errors
    ///
    /// Returns an error when either duration is zero or negative.
    pub const fn new(
        debounce: Duration,
        maximum_age: Duration,
    ) -> Result<Self, BatchScheduleError> {
        if !debounce.is_positive() {
            return Err(BatchScheduleError::InvalidDebounce);
        }
        if !maximum_age.is_positive() {
            return Err(BatchScheduleError::InvalidMaximumAge);
        }
        Ok(Self {
            debounce,
            maximum_age,
        })
    }

    /// Returns the inactivity interval that closes a nonempty batch.
    #[must_use]
    pub const fn debounce(self) -> Duration {
        self.debounce
    }

    /// Returns the maximum time the oldest pending request may wait.
    #[must_use]
    pub const fn maximum_age(self) -> Duration {
        self.maximum_age
    }

    /// Evaluates one fixed pending snapshot at a caller-supplied time.
    #[must_use]
    pub fn readiness(
        self,
        snapshot: PendingSnapshot,
        maximum_requests: NonZeroUsize,
        now: OffsetDateTime,
    ) -> BatchReadiness {
        readiness_for(
            snapshot.requests(),
            snapshot.has_invalid_acceptance(),
            snapshot.oldest_accepted_at(),
            snapshot.newest_accepted_at(),
            maximum_requests,
            self,
            now,
        )
    }
}

fn readiness_for(
    requests: usize,
    has_invalid_acceptance: bool,
    oldest_accepted_at: Option<OffsetDateTime>,
    newest_accepted_at: Option<OffsetDateTime>,
    maximum_requests: NonZeroUsize,
    schedule: BatchSchedule,
    now: OffsetDateTime,
) -> BatchReadiness {
    if requests == 0 {
        return BatchReadiness::Empty;
    }
    if has_invalid_acceptance {
        return BatchReadiness::Ready {
            reason: BatchCloseReason::InvalidAcceptance,
        };
    }
    if requests >= maximum_requests.get() {
        return BatchReadiness::Ready {
            reason: BatchCloseReason::MaximumRequests,
        };
    }
    let Some(oldest) = oldest_accepted_at else {
        return BatchReadiness::Ready {
            reason: BatchCloseReason::InvalidAcceptance,
        };
    };
    let Some(newest) = newest_accepted_at else {
        return BatchReadiness::Ready {
            reason: BatchCloseReason::InvalidAcceptance,
        };
    };
    let maximum_age_at = oldest.saturating_add(schedule.maximum_age);
    if now >= maximum_age_at {
        return BatchReadiness::Ready {
            reason: BatchCloseReason::MaximumAge,
        };
    }
    let debounce_at = newest.saturating_add(schedule.debounce);
    if now >= debounce_at {
        return BatchReadiness::Ready {
            reason: BatchCloseReason::Debounce,
        };
    }
    BatchReadiness::Waiting {
        ready_at: maximum_age_at.min(debounce_at),
    }
}

impl Default for BatchSchedule {
    fn default() -> Self {
        match Self::new(Duration::seconds(30), Duration::minutes(5)) {
            Ok(schedule) => schedule,
            Err(error) => panic!("built-in batch schedule must be valid: {error}"),
        }
    }
}

/// Current decision for one fixed pending snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BatchReadiness {
    /// No pending requests were observed.
    Empty,
    /// The snapshot remains open until the earlier time threshold.
    Waiting {
        /// Earliest time at which the same snapshot becomes ready.
        ready_at: OffsetDateTime,
    },
    /// The snapshot must close now.
    Ready {
        /// Threshold responsible for closing the batch.
        reason: BatchCloseReason,
    },
}

/// Threshold responsible for closing one Worker batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BatchCloseReason {
    /// At least one pending entry has invalid acceptance metadata.
    InvalidAcceptance,
    /// The configured request-count limit was reached.
    MaximumRequests,
    /// The oldest pending request reached its maximum wait.
    MaximumAge,
    /// No request arrived during the configured inactivity interval.
    Debounce,
}

/// Invalid Worker batch scheduling configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BatchScheduleError {
    /// The debounce interval was zero or negative.
    InvalidDebounce,
    /// The maximum batch age was zero or negative.
    InvalidMaximumAge,
}

impl fmt::Display for BatchScheduleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidDebounce => {
                formatter.write_str("batch debounce interval must be greater than zero")
            }
            Self::InvalidMaximumAge => {
                formatter.write_str("maximum batch age must be greater than zero")
            }
        }
    }
}

impl std::error::Error for BatchScheduleError {}

#[cfg(test)]
mod tests;

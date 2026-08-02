//! Durable request-package validation and queue storage.

mod file_queue;
mod package;

pub use file_queue::{
    BatchClaimOutcome, BatchReconciliation, CURRENT_WORKER_PHASE_SCHEMA_VERSION,
    CURRENT_WORKER_RESULT_SCHEMA_VERSION, ClaimToken, ClaimedPackage, EnqueueOutcome, FileQueue,
    IncomingPackage, PendingScanOutcome, PendingSnapshot, ProcessingScanOutcome, QueueError,
    QueueLimit, QueueOverview, QueueReader, QueueRequestStatus, QueueState, WorkerPhase,
    WorkerPhaseRecord, WorkerQueueError, WorkerResultRecord, WorkerResultStatus, WorkerSession,
};
pub use package::{
    AcceptanceMetadata, MarkdownValidationError, PackageDigest, PackageLimit, PackageLimits,
    PackagePolicy, PackagePolicyError, PackageValidationError, PayloadMetadata, ValidatedPackage,
    validate_accepted_package, validate_package,
};

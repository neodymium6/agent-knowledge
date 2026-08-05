//! Durable request-package validation and queue storage.

mod file_queue;
mod package;

pub use file_queue::{
    BatchClaimOutcome, BatchReconciliation, CURRENT_WORKER_PHASE_SCHEMA_VERSION,
    CURRENT_WORKER_RESULT_SCHEMA_VERSION, ClaimToken, ClaimedPackage, EnqueueOutcome, FileQueue,
    IncomingPackage, PendingScanOutcome, PendingSnapshot, ProcessingScanOutcome, QueueError,
    QueueLimit, QueueOperationDeadline, QueueOverview, QueueReader, QueueRequestStatus, QueueState,
    WorkerPhase, WorkerPhaseRecord, WorkerQueueError, WorkerResultRecord, WorkerResultStatus,
    WorkerSession,
};
#[cfg(target_os = "linux")]
pub use file_queue::{migrate_legacy_queue_binding, rebind_restored_queue};
pub use package::{
    AcceptanceMetadata, MarkdownValidationError, PackageDigest, PackageLimit, PackageLimits,
    PackagePolicy, PackagePolicyError, PackageValidationError, PayloadMetadata, ValidatedPackage,
    validate_accepted_package, validate_package,
};

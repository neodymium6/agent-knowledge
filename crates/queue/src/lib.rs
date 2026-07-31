//! Durable request-package validation and queue storage.

mod file_queue;
mod package;

pub use file_queue::{
    CURRENT_WORKER_PHASE_SCHEMA_VERSION, ClaimToken, ClaimedPackage, EnqueueOutcome, FileQueue,
    IncomingPackage, QueueError, QueueLimit, QueueState, WorkerPhase, WorkerPhaseRecord,
    WorkerQueueError,
};
pub use package::{
    AcceptanceMetadata, MarkdownValidationError, PackageDigest, PackageLimit, PackageLimits,
    PackagePolicy, PackagePolicyError, PackageValidationError, PayloadMetadata, ValidatedPackage,
    validate_accepted_package, validate_package,
};

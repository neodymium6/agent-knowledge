//! Canonical content indexing and Repository Worker transactions.

mod apply;
mod git;
mod index;

pub use apply::{ApplyError, ApplyOutcome, apply_claimed};
pub use git::{
    BatchCommitOutcome, GitIdentity, GitRepository, GitTransactionError, RequestFailure,
};
pub use index::{
    ContentIndex, ContentIndexError, ContentPolicy, DocumentLocation, DocumentRecord,
    RevisionCheckError,
};

//! Canonical content indexing and Repository Worker transactions.

mod apply;
mod git;
mod index;
mod read;
mod replication;

pub use apply::ApplyError;
pub use git::{
    BatchCommitOutcome, BatchPublication, ClaimedBatch, GitIdentity, GitRepository,
    GitTransactionError, PublicationError, RepositoryTransaction, RequestFailure,
    trusted_git_program,
};
pub use index::{
    AttachmentRecord, ContentIndex, ContentIndexError, ContentPolicy, DocumentLocation,
    DocumentRecord, RevisionCheckError,
};
pub use read::{
    CommittedBundle, CommittedBundleEntry, CommittedDocument, CommittedReadError,
    CommittedSnapshot, CommittedStore, LinearSearch, ReadFilter, SearchBackend,
    SearchMetadataFields, SearchPolicy,
};
pub use replication::{
    RemoteReplicationError, RemoteReplicationOutcome, RemoteReplicationPolicy,
    RemoteReplicationStatus, RemoteReplicator, read_remote_replication_status,
    read_remote_replication_status_until,
};

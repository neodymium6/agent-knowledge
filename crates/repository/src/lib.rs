//! Canonical content indexing and Repository Worker transactions.

mod index;

pub use index::{
    ContentIndex, ContentIndexError, ContentPolicy, DocumentRecord, RevisionCheckError,
};

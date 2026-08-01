//! Shared domain types and validation for Agent Knowledge.

mod document;
mod error_code;
mod filesystem;
mod id;
mod input;
mod markdown;
mod path;
mod request;
mod revision;

pub use document::{
    CURRENT_DOCUMENT_SCHEMA_VERSION, DocumentLimits, DocumentMetadata, DocumentValidationError,
};
pub use error_code::{DocumentStatus, ErrorCode};
pub use filesystem::{PathAttestation, PathAttestationError};
pub use id::{BatchId, DocumentId, RequestId, SessionId};
pub use input::{BoundedFileError, read_bounded_regular_file};
pub use markdown::{DocumentParseError, decode_document_metadata};
pub use path::{AttachmentName, PathValidationError, PayloadPath, ProjectId};
pub use request::{
    CURRENT_PROTOCOL_VERSION, ChangeRequest, DocumentType, Operation, RequestDecodeError,
    RequestLimits, RequestValidationError,
};
pub use revision::{Revision, RevisionParseError};

/// The stable application name used by commands and diagnostics.
pub const APPLICATION_NAME: &str = "agent-knowledge";

#[cfg(test)]
mod tests {
    use super::APPLICATION_NAME;

    #[test]
    fn application_name_is_stable() {
        assert_eq!(APPLICATION_NAME, "agent-knowledge");
    }
}

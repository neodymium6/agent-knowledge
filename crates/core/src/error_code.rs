use std::fmt;

use serde::{Deserialize, Serialize};

/// The lifecycle status recorded in document front matter.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DocumentStatus {
    /// The document is current and mutable according to its type.
    Active,
    /// The document represents completed work.
    Completed,
    /// The document has been replaced or should no longer be used.
    Deprecated,
    /// The document has been moved out of active classification.
    Archived,
}

/// A stable machine-readable failure code.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    /// The protocol version or wire command is unsupported.
    InvalidProtocol,
    /// The change request structure or metadata is invalid.
    InvalidRequest,
    /// Markdown front matter is invalid.
    InvalidFrontMatter,
    /// A requested or payload path is invalid.
    InvalidPath,
    /// An attachment type is not allowed.
    UnsupportedFileType,
    /// A configured or protocol limit was exceeded.
    LimitExceeded,
    /// A request ID was reused with different content.
    RequestIdReused,
    /// A requested change request does not exist in durable queue state.
    RequestNotFound,
    /// A document ID already exists.
    DocumentIdConflict,
    /// A requested document does not exist.
    DocumentNotFound,
    /// The current document revision differs from the expected revision.
    RevisionConflict,
    /// The operation is forbidden for the document type or state.
    OperationForbidden,
    /// The resulting content hierarchy failed validation.
    ContentValidationFailed,
    /// A deterministic or transient Quartz build failed.
    QuartzBuildFailed,
    /// A retryable infrastructure operation failed.
    TemporaryFailure,
    /// An unexpected implementation failure occurred.
    InternalError,
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::InvalidProtocol => "INVALID_PROTOCOL",
            Self::InvalidRequest => "INVALID_REQUEST",
            Self::InvalidFrontMatter => "INVALID_FRONT_MATTER",
            Self::InvalidPath => "INVALID_PATH",
            Self::UnsupportedFileType => "UNSUPPORTED_FILE_TYPE",
            Self::LimitExceeded => "LIMIT_EXCEEDED",
            Self::RequestIdReused => "REQUEST_ID_REUSED",
            Self::RequestNotFound => "REQUEST_NOT_FOUND",
            Self::DocumentIdConflict => "DOCUMENT_ID_CONFLICT",
            Self::DocumentNotFound => "DOCUMENT_NOT_FOUND",
            Self::RevisionConflict => "REVISION_CONFLICT",
            Self::OperationForbidden => "OPERATION_FORBIDDEN",
            Self::ContentValidationFailed => "CONTENT_VALIDATION_FAILED",
            Self::QuartzBuildFailed => "QUARTZ_BUILD_FAILED",
            Self::TemporaryFailure => "TEMPORARY_FAILURE",
            Self::InternalError => "INTERNAL_ERROR",
        };
        formatter.write_str(value)
    }
}

#[cfg(test)]
mod tests {
    use super::{DocumentStatus, ErrorCode};

    #[test]
    fn error_codes_have_stable_wire_names() {
        let cases = [
            (ErrorCode::InvalidProtocol, "INVALID_PROTOCOL"),
            (ErrorCode::InvalidRequest, "INVALID_REQUEST"),
            (ErrorCode::InvalidFrontMatter, "INVALID_FRONT_MATTER"),
            (ErrorCode::InvalidPath, "INVALID_PATH"),
            (ErrorCode::UnsupportedFileType, "UNSUPPORTED_FILE_TYPE"),
            (ErrorCode::LimitExceeded, "LIMIT_EXCEEDED"),
            (ErrorCode::RequestIdReused, "REQUEST_ID_REUSED"),
            (ErrorCode::RequestNotFound, "REQUEST_NOT_FOUND"),
            (ErrorCode::DocumentIdConflict, "DOCUMENT_ID_CONFLICT"),
            (ErrorCode::DocumentNotFound, "DOCUMENT_NOT_FOUND"),
            (ErrorCode::RevisionConflict, "REVISION_CONFLICT"),
            (ErrorCode::OperationForbidden, "OPERATION_FORBIDDEN"),
            (
                ErrorCode::ContentValidationFailed,
                "CONTENT_VALIDATION_FAILED",
            ),
            (ErrorCode::QuartzBuildFailed, "QUARTZ_BUILD_FAILED"),
            (ErrorCode::TemporaryFailure, "TEMPORARY_FAILURE"),
            (ErrorCode::InternalError, "INTERNAL_ERROR"),
        ];

        for (code, expected) in cases {
            let serialized = match serde_json::to_string(&code) {
                Ok(serialized) => serialized,
                Err(error) => panic!("error code must serialize: {error}"),
            };
            assert_eq!(serialized, format!("\"{expected}\""));
            assert_eq!(code.to_string(), expected);
        }
    }

    #[test]
    fn document_statuses_use_snake_case() {
        let serialized = match serde_json::to_string(&DocumentStatus::Deprecated) {
            Ok(serialized) => serialized,
            Err(error) => panic!("document status must serialize: {error}"),
        };
        assert_eq!(serialized, "\"deprecated\"");
    }
}

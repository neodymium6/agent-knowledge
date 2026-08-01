use agent_knowledge_core::{ErrorCode, RequestId};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::CURRENT_GATEWAY_PROTOCOL_VERSION;

/// Input for one durable change-request status lookup.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StatusRequest {
    /// Independent Gateway protocol version.
    pub protocol_version: u16,
    /// Permanent identity assigned to the submitted change request.
    pub request_id: RequestId,
}

impl StatusRequest {
    /// Constructs a request using the current protocol version.
    #[must_use]
    pub const fn new(request_id: RequestId) -> Self {
        Self {
            protocol_version: CURRENT_GATEWAY_PROTOCOL_VERSION,
            request_id,
        }
    }
}

/// Durable lifecycle state returned for one accepted change request.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum RequestStatus {
    /// Accepted and awaiting the Repository Worker.
    Pending,
    /// Claimed by the Repository Worker.
    Processing,
    /// Committed and published locally.
    Completed,
    /// Permanently rejected and retained with failure metadata.
    Failed {
        /// Stable machine-readable failure classification.
        error_code: ErrorCode,
        /// Central-server time when the failure became durable.
        #[serde(with = "time::serde::rfc3339")]
        failed_at: OffsetDateTime,
    },
}

/// Successful response for one durable change-request status lookup.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum StatusResponse {
    /// Accepted and awaiting the Repository Worker.
    Pending {
        /// Independent Gateway protocol version.
        protocol_version: u16,
        /// Identity of the observed change request.
        request_id: RequestId,
    },
    /// Claimed by the Repository Worker.
    Processing {
        /// Independent Gateway protocol version.
        protocol_version: u16,
        /// Identity of the observed change request.
        request_id: RequestId,
    },
    /// Committed and published locally.
    Completed {
        /// Independent Gateway protocol version.
        protocol_version: u16,
        /// Identity of the observed change request.
        request_id: RequestId,
    },
    /// Permanently rejected and retained with failure metadata.
    Failed {
        /// Independent Gateway protocol version.
        protocol_version: u16,
        /// Identity of the observed change request.
        request_id: RequestId,
        /// Stable machine-readable failure classification.
        error_code: ErrorCode,
        /// Central-server time when the failure became durable.
        #[serde(with = "time::serde::rfc3339")]
        failed_at: OffsetDateTime,
    },
}

impl StatusResponse {
    /// Constructs a response using the current protocol version.
    #[must_use]
    pub const fn new(request_id: RequestId, status: RequestStatus) -> Self {
        match status {
            RequestStatus::Pending => Self::Pending {
                protocol_version: CURRENT_GATEWAY_PROTOCOL_VERSION,
                request_id,
            },
            RequestStatus::Processing => Self::Processing {
                protocol_version: CURRENT_GATEWAY_PROTOCOL_VERSION,
                request_id,
            },
            RequestStatus::Completed => Self::Completed {
                protocol_version: CURRENT_GATEWAY_PROTOCOL_VERSION,
                request_id,
            },
            RequestStatus::Failed {
                error_code,
                failed_at,
            } => Self::Failed {
                protocol_version: CURRENT_GATEWAY_PROTOCOL_VERSION,
                request_id,
                error_code,
                failed_at,
            },
        }
    }

    /// Returns the independent Gateway protocol version.
    #[must_use]
    pub const fn protocol_version(self) -> u16 {
        match self {
            Self::Pending {
                protocol_version, ..
            }
            | Self::Processing {
                protocol_version, ..
            }
            | Self::Completed {
                protocol_version, ..
            }
            | Self::Failed {
                protocol_version, ..
            } => protocol_version,
        }
    }

    /// Returns the identity of the observed change request.
    #[must_use]
    pub const fn request_id(self) -> RequestId {
        match self {
            Self::Pending { request_id, .. }
            | Self::Processing { request_id, .. }
            | Self::Completed { request_id, .. }
            | Self::Failed { request_id, .. } => request_id,
        }
    }

    /// Returns the durable lifecycle state and state-specific metadata.
    #[must_use]
    pub const fn request_status(self) -> RequestStatus {
        match self {
            Self::Pending { .. } => RequestStatus::Pending,
            Self::Processing { .. } => RequestStatus::Processing,
            Self::Completed { .. } => RequestStatus::Completed,
            Self::Failed {
                error_code,
                failed_at,
                ..
            } => RequestStatus::Failed {
                error_code,
                failed_at,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{RequestStatus, StatusRequest, StatusResponse};
    use agent_knowledge_core::ErrorCode;

    #[test]
    fn status_wire_types_are_strict_and_versioned() {
        let request_id = "01K00000000000000000000000"
            .parse()
            .unwrap_or_else(|error| panic!("request ID fixture must parse: {error}"));
        let request = StatusRequest::new(request_id);
        let encoded = serde_json::to_string(&request)
            .unwrap_or_else(|error| panic!("status request must encode: {error}"));
        assert_eq!(
            encoded,
            r#"{"protocol_version":1,"request_id":"01K00000000000000000000000"}"#
        );
        assert!(
            serde_json::from_str::<StatusRequest>(
                r#"{"protocol_version":1,"request_id":"01K00000000000000000000000","extra":true}"#
            )
            .is_err()
        );

        let response = StatusResponse::new(
            request_id,
            RequestStatus::Failed {
                error_code: ErrorCode::RevisionConflict,
                failed_at: time::OffsetDateTime::parse(
                    "2026-07-31T04:00:00Z",
                    &time::format_description::well_known::Rfc3339,
                )
                .unwrap_or_else(|error| panic!("failure time fixture must parse: {error}")),
            },
        );
        let encoded = serde_json::to_string(&response)
            .unwrap_or_else(|error| panic!("status response must encode: {error}"));
        assert_eq!(
            encoded,
            r#"{"status":"failed","protocol_version":1,"request_id":"01K00000000000000000000000","error_code":"REVISION_CONFLICT","failed_at":"2026-07-31T04:00:00Z"}"#
        );
        assert!(serde_json::from_str::<StatusResponse>(&encoded).is_ok());
        assert!(
            serde_json::from_str::<StatusResponse>(&encoded.replace('}', ",\"extra\":true}"))
                .is_err()
        );
    }
}

//! Versioned SSH command and JSON wire types shared by Gateway and client.

mod read;
mod status;

use std::ffi::OsStr;
use std::fmt;
use std::str::FromStr;

use agent_knowledge_core::{ErrorCode, RequestId, Revision};
use serde::{Deserialize, Serialize};

pub use read::{
    DocumentContent, DocumentSummary, ExportRequest, GetRequest, GetResponse, ListRequest,
    ListResponse, ReadFilterRequest, SearchRequest,
};
pub use status::{RequestStatus, StatusRequest, StatusResponse};

/// The protocol version encoded in Gateway commands and JSON responses.
pub const CURRENT_GATEWAY_PROTOCOL_VERSION: u16 = 1;
/// The exact remote command used to submit one request package.
pub const SUBMIT_COMMAND: &str = "akp-v1 submit";
/// The exact remote command used to list committed documents.
pub const LIST_COMMAND: &str = "akp-v1 list";
/// The exact remote command used to list recently changed committed documents.
pub const RECENT_COMMAND: &str = "akp-v1 recent";
/// The exact remote command used to retrieve one committed Markdown document.
pub const GET_COMMAND: &str = "akp-v1 get";
/// The exact remote command used to export one committed document bundle.
pub const EXPORT_COMMAND: &str = "akp-v1 export";
/// The exact remote command used to search committed documents.
pub const SEARCH_COMMAND: &str = "akp-v1 search";
/// The exact remote command used to inspect one durable request state.
pub const STATUS_COMMAND: &str = "akp-v1 status";
const MAXIMUM_CLIENT_ID_BYTES: usize = 63;

/// One operation selected by an authenticated SSH session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GatewayCommand {
    /// Streams one uncompressed request-package tar archive on standard input.
    Submit,
    /// Lists committed document metadata.
    List,
    /// Lists committed documents by effective update time.
    Recent,
    /// Retrieves one committed Markdown document.
    Get,
    /// Exports one committed document bundle as an uncompressed tar stream.
    Export,
    /// Searches committed Markdown and permitted metadata.
    Search,
    /// Retrieves one durable change-request status.
    Status,
}

impl GatewayCommand {
    /// Parses an exact versioned command without shell tokenization.
    ///
    /// # Errors
    ///
    /// Returns [`GatewayCommandError`] for missing, malformed, or unsupported
    /// commands, including leading or trailing whitespace.
    pub fn parse(original: &OsStr) -> Result<Self, GatewayCommandError> {
        match original {
            value if value == OsStr::new(SUBMIT_COMMAND) => Ok(Self::Submit),
            value if value == OsStr::new(LIST_COMMAND) => Ok(Self::List),
            value if value == OsStr::new(RECENT_COMMAND) => Ok(Self::Recent),
            value if value == OsStr::new(GET_COMMAND) => Ok(Self::Get),
            value if value == OsStr::new(EXPORT_COMMAND) => Ok(Self::Export),
            value if value == OsStr::new(SEARCH_COMMAND) => Ok(Self::Search),
            value if value == OsStr::new(STATUS_COMMAND) => Ok(Self::Status),
            _ => Err(GatewayCommandError),
        }
    }

    /// Returns the exact command sent by the official SSH client.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Submit => SUBMIT_COMMAND,
            Self::List => LIST_COMMAND,
            Self::Recent => RECENT_COMMAND,
            Self::Get => GET_COMMAND,
            Self::Export => EXPORT_COMMAND,
            Self::Search => SEARCH_COMMAND,
            Self::Status => STATUS_COMMAND,
        }
    }
}

/// An unsupported or malformed forced-command selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GatewayCommandError;

impl fmt::Display for GatewayCommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("unsupported Gateway command")
    }
}

impl std::error::Error for GatewayCommandError {}

/// A stable identifier assigned to one authenticated SSH public key.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(try_from = "String", into = "String")]
pub struct ClientId(String);

impl ClientId {
    /// Returns the identifier as lowercase ASCII text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ClientId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for ClientId {
    type Err = ClientIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() {
            return Err(ClientIdError::Empty);
        }
        if value.len() > MAXIMUM_CLIENT_ID_BYTES {
            return Err(ClientIdError::TooLong {
                maximum: MAXIMUM_CLIENT_ID_BYTES,
                actual: value.len(),
            });
        }
        if value.starts_with('-')
            || value.ends_with('-')
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err(ClientIdError::InvalidCharacter);
        }
        Ok(Self(value.into()))
    }
}

impl TryFrom<String> for ClientId {
    type Error = ClientIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<ClientId> for String {
    fn from(value: ClientId) -> Self {
        value.0
    }
}

/// An invalid authenticated client identifier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientIdError {
    /// The identifier was empty.
    Empty,
    /// The identifier exceeded the fixed protocol bound.
    TooLong {
        /// Maximum accepted bytes.
        maximum: usize,
        /// Observed bytes.
        actual: usize,
    },
    /// The identifier was not a lowercase ASCII slug.
    InvalidCharacter,
}

impl fmt::Display for ClientIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("client ID must not be empty"),
            Self::TooLong { maximum, actual } => write!(
                formatter,
                "client ID has {actual} bytes; maximum is {maximum}"
            ),
            Self::InvalidCharacter => {
                formatter.write_str("client ID must be a lowercase ASCII slug without edge hyphens")
            }
        }
    }
}

impl std::error::Error for ClientIdError {}

/// A queue state visible in an idempotent submission response.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestState {
    /// Accepted and awaiting the Worker.
    Pending,
    /// Claimed by the Worker.
    Processing,
    /// Committed and published.
    Completed,
    /// Permanently rejected and retained.
    Failed,
}

/// One successful Gateway submission response.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SubmitResponse {
    /// Independent Gateway protocol version.
    pub protocol_version: u16,
    /// Durable acceptance outcome.
    #[serde(flatten)]
    pub outcome: SubmitOutcome,
}

impl SubmitResponse {
    /// Constructs a response using the current protocol version.
    #[must_use]
    pub const fn new(outcome: SubmitOutcome) -> Self {
        Self {
            protocol_version: CURRENT_GATEWAY_PROTOCOL_VERSION,
            outcome,
        }
    }
}

/// Durable acceptance result for one request ID and normalized digest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum SubmitOutcome {
    /// A new request package was durably accepted.
    Accepted {
        /// Accepted request identifier.
        request_id: RequestId,
        /// SHA-256 digest of normalized client-controlled package contents.
        digest: Revision,
    },
    /// The same immutable request package was already accepted.
    Existing {
        /// Existing request identifier.
        request_id: RequestId,
        /// SHA-256 digest of normalized client-controlled package contents.
        digest: Revision,
        /// Current durable queue state.
        state: RequestState,
    },
}

/// A machine-readable Gateway failure written to standard error.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolErrorResponse {
    /// Independent Gateway protocol version.
    pub protocol_version: u16,
    /// Stable error classification.
    pub error_code: ErrorCode,
}

impl ProtocolErrorResponse {
    /// Constructs an error response using the current protocol version.
    #[must_use]
    pub const fn new(error_code: ErrorCode) -> Self {
        Self {
            protocol_version: CURRENT_GATEWAY_PROTOCOL_VERSION,
            error_code,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;

    use agent_knowledge_core::ErrorCode;

    use super::{
        CURRENT_GATEWAY_PROTOCOL_VERSION, ClientId, EXPORT_COMMAND, GET_COMMAND, GatewayCommand,
        LIST_COMMAND, ProtocolErrorResponse, RECENT_COMMAND, SEARCH_COMMAND, STATUS_COMMAND,
        SUBMIT_COMMAND,
    };

    #[test]
    fn command_parser_accepts_only_the_exact_versioned_grammar() {
        assert_eq!(
            GatewayCommand::parse(OsStr::new(SUBMIT_COMMAND)),
            Ok(GatewayCommand::Submit)
        );
        for (command, expected) in [
            (LIST_COMMAND, GatewayCommand::List),
            (RECENT_COMMAND, GatewayCommand::Recent),
            (GET_COMMAND, GatewayCommand::Get),
            (EXPORT_COMMAND, GatewayCommand::Export),
            (SEARCH_COMMAND, GatewayCommand::Search),
            (STATUS_COMMAND, GatewayCommand::Status),
        ] {
            assert_eq!(GatewayCommand::parse(OsStr::new(command)), Ok(expected));
            assert_eq!(expected.as_str(), command);
        }
        for rejected in [
            "",
            "akp-v1",
            "akp-v1  submit",
            "akp-v1 submit ",
            " akp-v1 submit",
            "akp-v2 submit",
            "akp-v1 statuses",
            "akp-v1 submit; id",
        ] {
            assert!(GatewayCommand::parse(OsStr::new(rejected)).is_err());
        }
    }

    #[test]
    fn client_ids_are_bounded_lowercase_ascii_slugs() {
        assert!("fictional-node-a".parse::<ClientId>().is_ok());
        for rejected in ["", "-node", "node-", "Node", "node_a", "node/a"] {
            assert!(rejected.parse::<ClientId>().is_err());
        }
        assert!("a".repeat(64).parse::<ClientId>().is_err());
    }

    #[test]
    fn error_response_has_a_strict_versioned_shape() {
        let encoded =
            serde_json::to_string(&ProtocolErrorResponse::new(ErrorCode::InvalidProtocol))
                .unwrap_or_else(|error| panic!("protocol error fixture must encode: {error}"));
        assert_eq!(
            encoded,
            format!(
                "{{\"protocol_version\":{CURRENT_GATEWAY_PROTOCOL_VERSION},\"error_code\":\"INVALID_PROTOCOL\"}}"
            )
        );
        assert!(
            serde_json::from_str::<ProtocolErrorResponse>(
                "{\"protocol_version\":1,\"error_code\":\"INVALID_PROTOCOL\",\"extra\":true}"
            )
            .is_err()
        );
    }

    #[test]
    fn submit_response_round_trips_and_rejects_unknown_fields() {
        let response = "{\"protocol_version\":1,\"status\":\"accepted\",\"request_id\":\"01K00000000000000000000000\",\"digest\":\"sha256:0000000000000000000000000000000000000000000000000000000000000000\"}";
        assert!(serde_json::from_str::<super::SubmitResponse>(response).is_ok());
        let response_with_extra = response.replace("}", ",\"extra\":true}");
        assert!(serde_json::from_str::<super::SubmitResponse>(&response_with_extra).is_err());
    }
}

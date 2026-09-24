use serde::{Deserialize, Serialize};

use crate::CURRENT_GATEWAY_PROTOCOL_VERSION;

/// Read-only input for reporting the running Gateway binary.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VersionRequest {
    /// Wire protocol version, independent of the software release.
    pub protocol_version: u16,
}

impl Default for VersionRequest {
    fn default() -> Self {
        Self {
            protocol_version: CURRENT_GATEWAY_PROTOCOL_VERSION,
        }
    }
}

/// The Gateway binary version and explicitly advertised operations.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VersionResponse {
    /// Wire protocol version, independent of the software release.
    pub protocol_version: u16,
    /// Version of the running Gateway, not an inferred Worker version.
    pub gateway_version: String,
    /// Exact commands supported by this Gateway.
    pub commands: Vec<String>,
    /// Supported query kinds within the inspect command.
    pub inspect_queries: Vec<String>,
}

impl VersionResponse {
    /// Describes this build's Gateway protocol capabilities.
    #[must_use]
    pub fn current(gateway_version: &str) -> Self {
        use crate::GatewayCommand;
        Self {
            protocol_version: CURRENT_GATEWAY_PROTOCOL_VERSION,
            gateway_version: gateway_version.to_owned(),
            commands: [
                GatewayCommand::Submit,
                GatewayCommand::List,
                GatewayCommand::Recent,
                GatewayCommand::Get,
                GatewayCommand::Export,
                GatewayCommand::Search,
                GatewayCommand::Status,
                GatewayCommand::Inspect,
                GatewayCommand::Version,
            ]
            .into_iter()
            .map(|command| command.as_str().to_owned())
            .collect(),
            inspect_queries: [
                "search_excerpts",
                "context",
                "context_balanced",
                "history",
                "get_at",
                "diff",
                "diff_hunks",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_request_is_strict_and_capabilities_are_parseable() {
        assert!(
            serde_json::from_str::<VersionRequest>(r#"{"protocol_version":1,"extra":true}"#)
                .is_err()
        );
        let response = VersionResponse::current("0.1.0");
        assert_eq!(
            response.protocol_version,
            VersionRequest::default().protocol_version
        );
        for command in response.commands {
            assert!(crate::GatewayCommand::parse(std::ffi::OsStr::new(&command)).is_ok());
        }
        assert_eq!(response.inspect_queries.len(), 7);
    }
}

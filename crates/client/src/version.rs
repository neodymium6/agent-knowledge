//! Software versions are reported independently of wire protocol capabilities.
use std::ffi::OsStr;
use std::time::Duration;

use agent_knowledge_core::ErrorCode;
use agent_knowledge_protocol::{
    CURRENT_GATEWAY_PROTOCOL_VERSION, VERSION_COMMAND, VersionRequest, VersionResponse,
};
use serde::Serialize;

use crate::{
    ClientCommandError, ControlOperation, SSH_PROGRAM, SshClient, execute_control_with_program,
};

/// A bounded, best-effort observation of a connected Gateway.
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ServerVersion {
    /// No SSH destination was requested.
    NotRequested,
    /// An older Gateway rejected the additive command.
    Unsupported,
    /// Transport or response validation failed; no version is inferred.
    Unavailable,
    /// The Gateway explicitly advertised its software and capabilities.
    Available {
        /// Exact response from the Gateway.
        #[serde(flatten)]
        gateway: VersionResponse,
        /// Whether the reported wire protocol matches this client.
        protocol_matches: bool,
    },
}

/// Local and remote versions, without inferring compatibility from release numbers.
#[derive(Clone, Debug, Serialize)]
pub struct VersionReport {
    /// Version embedded in the client binary.
    pub client_version: &'static str,
    /// Wire protocol implemented by this client.
    pub client_protocol_version: u16,
    /// Best-effort Gateway observation.
    pub server: ServerVersion,
}

impl VersionReport {
    /// Creates a report from a bounded Gateway observation.
    #[must_use]
    pub const fn new(server: ServerVersion) -> Self {
        Self {
            client_version: env!("CARGO_PKG_VERSION"),
            client_protocol_version: CURRENT_GATEWAY_PROTOCOL_VERSION,
            server,
        }
    }
}

impl SshClient {
    /// Reports the Gateway within five seconds, including explicit unknown states.
    #[must_use]
    pub fn server_version(&self) -> ServerVersion {
        self.server_version_with_program(OsStr::new(SSH_PROGRAM))
    }

    fn server_version_with_program(&self, program: &OsStr) -> ServerVersion {
        let result = execute_control_with_program(
            program,
            &self.destination,
            ControlOperation::new(VERSION_COMMAND, 16 * 1024),
            &VersionRequest::default(),
            self.timeout.min(Duration::from_secs(5)),
        );
        match result {
            Ok((bytes, _)) => decode_server(&bytes),
            Err(ClientCommandError::GatewayRejected(error))
                if error.error_code == ErrorCode::InvalidProtocol =>
            {
                ServerVersion::Unsupported
            }
            Err(_) => ServerVersion::Unavailable,
        }
    }
}

fn decode_server(bytes: &[u8]) -> ServerVersion {
    let Ok(gateway) = serde_json::from_slice::<VersionResponse>(bytes) else {
        return ServerVersion::Unavailable;
    };
    let safe = |text: &str| {
        !text.is_empty()
            && text.len() <= 128
            && text.bytes().all(|b| b.is_ascii_graphic() || b == b' ')
    };
    if !safe(&gateway.gateway_version)
        || gateway.commands.len() > 64
        || gateway.inspect_queries.len() > 64
        || !gateway
            .commands
            .iter()
            .chain(&gateway.inspect_queries)
            .all(|value| safe(value))
    {
        return ServerVersion::Unavailable;
    }
    ServerVersion::Available {
        protocol_matches: gateway.protocol_version == CURRENT_GATEWAY_PROTOCOL_VERSION,
        gateway,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_skew_and_protocol_support_are_independent() {
        for version in ["0.0.1", "99.0.0"] {
            let mut response = VersionResponse::current(version);
            for protocol in [1, 2] {
                response.protocol_version = protocol;
                let bytes =
                    serde_json::to_vec(&response).unwrap_or_else(|e| panic!("fixture: {e}"));
                assert!(
                    matches!(decode_server(&bytes), ServerVersion::Available { protocol_matches, .. } if protocol_matches == (protocol == 1))
                );
            }
        }
        assert!(matches!(decode_server(b"{}"), ServerVersion::Unavailable));
    }

    #[cfg(unix)]
    #[test]
    fn legacy_gateway_is_explicitly_unsupported() {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir().unwrap_or_else(|e| panic!("fixture: {e}"));
        let ssh = temp.path().join("ssh");
        std::fs::write(&ssh, "#!/bin/sh\ncat >/dev/null\nprintf '%s\\n' '{\"protocol_version\":1,\"error_code\":\"INVALID_PROTOCOL\"}' >&2\nexit 1\n").unwrap_or_else(|e| panic!("fixture: {e}"));
        std::fs::set_permissions(&ssh, std::fs::Permissions::from_mode(0o700))
            .unwrap_or_else(|e| panic!("fixture: {e}"));
        let client = SshClient::new("fictional-gateway", Duration::from_secs(2))
            .unwrap_or_else(|e| panic!("fixture: {e}"));
        assert!(matches!(
            client.server_version_with_program(ssh.as_os_str()),
            ServerVersion::Unsupported
        ));
        assert!(matches!(
            client.server_version_with_program(temp.path().join("absent").as_os_str()),
            ServerVersion::Unavailable
        ));
    }
}

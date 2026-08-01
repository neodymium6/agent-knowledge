use std::ffi::{OsStr, OsString};
use std::fmt;
use std::io::{self, Read, Write};
use std::path::Path;

use agent_knowledge_core::ErrorCode;
use agent_knowledge_gateway::{Gateway, GatewayConfigError, GatewayError, GatewaySettings};
use agent_knowledge_protocol::{ClientId, ClientIdError, GatewayCommand, ProtocolErrorResponse};

pub fn run<R, W>(
    config: &Path,
    client_id: &OsStr,
    original_command: Option<OsString>,
    input: R,
    mut output: W,
) -> Result<(), GatewayCommandError>
where
    R: Read,
    W: Write,
{
    let client_id = client_id
        .to_str()
        .ok_or(GatewayCommandError::InvalidClientIdEncoding)?
        .parse::<ClientId>()
        .map_err(GatewayCommandError::ClientId)?;
    let original_command = original_command.ok_or(GatewayCommandError::MissingCommand)?;
    let command = GatewayCommand::parse(&original_command)
        .map_err(|_| GatewayCommandError::InvalidCommand)?;
    let settings = GatewaySettings::load(config)
        .map_err(|error| GatewayCommandError::Config(Box::new(error)))?;
    let gateway =
        Gateway::open(&settings).map_err(|error| GatewayCommandError::Gateway(Box::new(error)))?;
    match command {
        GatewayCommand::Submit => {
            let response = gateway
                .submit(client_id, input)
                .map_err(|error| GatewayCommandError::Gateway(Box::new(error)))?;
            serde_json::to_writer(&mut output, &response).map_err(GatewayCommandError::Json)?;
            output.write_all(b"\n").map_err(GatewayCommandError::Io)
        }
    }
}

#[derive(Debug)]
pub enum GatewayCommandError {
    MissingCommand,
    InvalidCommand,
    InvalidClientIdEncoding,
    ClientId(ClientIdError),
    Config(Box<GatewayConfigError>),
    Gateway(Box<GatewayError>),
    Json(serde_json::Error),
    Io(io::Error),
}

impl GatewayCommandError {
    #[must_use]
    pub fn error_code(&self) -> ErrorCode {
        match self {
            Self::MissingCommand | Self::InvalidCommand => ErrorCode::InvalidProtocol,
            Self::Gateway(error) => error.error_code(),
            Self::InvalidClientIdEncoding
            | Self::ClientId(_)
            | Self::Config(_)
            | Self::Json(_)
            | Self::Io(_) => ErrorCode::InternalError,
        }
    }

    pub fn write_protocol_error(&self, mut output: impl Write) -> io::Result<()> {
        serde_json::to_writer(&mut output, &ProtocolErrorResponse::new(self.error_code()))
            .map_err(io::Error::other)?;
        output.write_all(b"\n")
    }
}

impl fmt::Display for GatewayCommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingCommand => formatter.write_str("SSH_ORIGINAL_COMMAND is missing"),
            Self::InvalidCommand => formatter.write_str("SSH_ORIGINAL_COMMAND is unsupported"),
            Self::InvalidClientIdEncoding => {
                formatter.write_str("forced-command client ID is not UTF-8")
            }
            Self::ClientId(error) => {
                write!(formatter, "forced-command client ID is invalid: {error}")
            }
            Self::Config(error) => write!(formatter, "Gateway configuration failed: {error}"),
            Self::Gateway(error) => error.fmt(formatter),
            Self::Json(error) => write!(formatter, "Gateway JSON encoding failed: {error}"),
            Self::Io(error) => write!(formatter, "Gateway response output failed: {error}"),
        }
    }
}

impl std::error::Error for GatewayCommandError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ClientId(error) => Some(error),
            Self::Config(error) => Some(error.as_ref()),
            Self::Gateway(error) => Some(error.as_ref()),
            Self::Json(error) => Some(error),
            Self::Io(error) => Some(error),
            Self::MissingCommand | Self::InvalidCommand | Self::InvalidClientIdEncoding => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;

    use agent_knowledge_core::ErrorCode;

    use super::GatewayCommandError;

    #[test]
    fn command_selection_failures_emit_only_a_versioned_protocol_error() {
        let error = GatewayCommandError::InvalidCommand;
        let mut output = Vec::new();
        error
            .write_protocol_error(&mut output)
            .unwrap_or_else(|write_error| panic!("protocol error must encode: {write_error}"));
        assert_eq!(
            std::str::from_utf8(&output),
            Ok("{\"protocol_version\":1,\"error_code\":\"INVALID_PROTOCOL\"}\n")
        );
        assert_eq!(error.error_code(), ErrorCode::InvalidProtocol);
        assert!(
            GatewayCommandError::MissingCommand
                .to_string()
                .contains("SSH_ORIGINAL_COMMAND")
        );
        assert!(OsStr::new("fictional-node-a").to_str().is_some());
    }
}

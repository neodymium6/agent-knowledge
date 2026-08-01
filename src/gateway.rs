use std::ffi::{OsStr, OsString};
use std::fmt;
use std::io::{self, Read, Write};
use std::path::Path;
#[cfg(target_os = "linux")]
use std::time::{Duration, Instant};

use agent_knowledge_core::ErrorCode;
use agent_knowledge_gateway::{Gateway, GatewayConfigError, GatewayError, GatewaySettings};
use agent_knowledge_protocol::{ClientId, ClientIdError, GatewayCommand, ProtocolErrorResponse};

#[cfg(target_os = "linux")]
pub fn run_stdio<W>(
    config: &Path,
    client_id: &OsStr,
    original_command: Option<OsString>,
    output: W,
) -> Result<(), GatewayCommandError>
where
    W: Write,
{
    let settings = GatewaySettings::load(config)
        .map_err(|error| GatewayCommandError::Config(Box::new(error)))?;
    let timeout = settings.submit_timeout();
    let stdin = io::stdin();
    let input = DeadlineReader::new(stdin.lock(), timeout);
    run_with_settings(settings, client_id, original_command, input, output)
}

#[cfg(not(target_os = "linux"))]
pub fn run_stdio<W>(
    config: &Path,
    client_id: &OsStr,
    original_command: Option<OsString>,
    output: W,
) -> Result<(), GatewayCommandError>
where
    W: Write,
{
    let settings = GatewaySettings::load(config)
        .map_err(|error| GatewayCommandError::Config(Box::new(error)))?;
    run_with_settings(
        settings,
        client_id,
        original_command,
        io::stdin().lock(),
        output,
    )
}

fn run_with_settings<R, W>(
    settings: GatewaySettings,
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

#[cfg(target_os = "linux")]
struct DeadlineReader<R> {
    inner: R,
    deadline: Instant,
}

#[cfg(target_os = "linux")]
impl<R> DeadlineReader<R> {
    fn new(inner: R, timeout: Duration) -> Self {
        Self {
            inner,
            deadline: Instant::now() + timeout,
        }
    }
}

#[cfg(target_os = "linux")]
impl<R> Read for DeadlineReader<R>
where
    R: Read + std::os::fd::AsFd,
{
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        use nix::errno::Errno;
        use nix::poll::{PollFd, PollFlags, PollTimeout, poll};

        if buffer.is_empty() {
            return Ok(0);
        }
        loop {
            let now = Instant::now();
            if now >= self.deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Gateway submit deadline expired",
                ));
            }
            let remaining = self.deadline.saturating_duration_since(now);
            let timeout = PollTimeout::try_from(remaining).unwrap_or(PollTimeout::MAX);
            let mut descriptors = [PollFd::new(self.inner.as_fd(), PollFlags::POLLIN)];
            match poll(&mut descriptors, timeout) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "Gateway submit deadline expired",
                    ));
                }
                Ok(_) => return self.inner.read(buffer),
                Err(Errno::EINTR) => {}
                Err(error) => return Err(io::Error::from_raw_os_error(error as i32)),
            }
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
    #[cfg(target_os = "linux")]
    use std::io::Read;
    #[cfg(target_os = "linux")]
    use std::os::unix::net::UnixStream;
    #[cfg(target_os = "linux")]
    use std::time::Duration;

    use agent_knowledge_core::ErrorCode;

    #[cfg(target_os = "linux")]
    use super::DeadlineReader;
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

    #[cfg(target_os = "linux")]
    #[test]
    fn submit_reader_enforces_an_absolute_idle_deadline() {
        let (reader, _writer) = UnixStream::pair()
            .unwrap_or_else(|error| panic!("deadline stream pair must open: {error}"));
        let mut reader = DeadlineReader::new(reader, Duration::from_millis(20));
        let mut byte = [0_u8; 1];
        let error = match reader.read(&mut byte) {
            Ok(_) => panic!("idle submit stream must time out"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    }
}

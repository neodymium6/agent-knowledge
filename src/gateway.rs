use std::ffi::{OsStr, OsString};
use std::fmt;
use std::io::{self, Read, Write};
#[cfg(target_os = "linux")]
use std::os::fd::AsFd;
use std::path::Path;
#[cfg(target_os = "linux")]
use std::time::Duration;
use std::time::Instant;

use agent_knowledge_core::ErrorCode;
use agent_knowledge_gateway::{
    GatewayConfigError, GatewayError, GatewaySettings, ReadGateway, StatusGateway, SubmitGateway,
};
use agent_knowledge_protocol::{ClientId, ClientIdError, GatewayCommand, ProtocolErrorResponse};
use serde::de::DeserializeOwned;

const MAXIMUM_CONTROL_REQUEST_BYTES: u64 = 64 * 1024;

#[cfg(target_os = "linux")]
pub fn run_stdio(
    config: &Path,
    client_id: &OsStr,
    original_command: Option<OsString>,
) -> Result<(), GatewayCommandError> {
    let settings = GatewaySettings::load(config)
        .map_err(|error| GatewayCommandError::Config(Box::new(error)))?;
    let timeout = settings.submit_timeout();
    let stdin = io::stdin();
    let input = std::fs::File::from(nix::unistd::dup(&stdin).map_err(|error| {
        GatewayCommandError::InputSetup(io::Error::from_raw_os_error(error as i32))
    })?);
    let stdout = io::stdout();
    let output = std::fs::File::from(nix::unistd::dup(&stdout).map_err(|error| {
        GatewayCommandError::OutputSetup(io::Error::from_raw_os_error(error as i32))
    })?);
    let input = DeadlineReader::new(input, timeout);
    run_with_settings(settings, client_id, original_command, input, output)
}

#[cfg(not(target_os = "linux"))]
pub fn run_stdio(
    config: &Path,
    client_id: &OsStr,
    original_command: Option<OsString>,
) -> Result<(), GatewayCommandError> {
    let settings = GatewaySettings::load(config)
        .map_err(|error| GatewayCommandError::Config(Box::new(error)))?;
    run_with_settings(
        settings,
        client_id,
        original_command,
        io::stdin().lock(),
        io::stdout(),
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
    W: Write + AsFd,
{
    let client_id = client_id
        .to_str()
        .ok_or(GatewayCommandError::InvalidClientIdEncoding)?
        .parse::<ClientId>()
        .map_err(GatewayCommandError::ClientId)?;
    let original_command = original_command.ok_or(GatewayCommandError::MissingCommand)?;
    let command = GatewayCommand::parse(&original_command)
        .map_err(|_| GatewayCommandError::InvalidCommand)?;
    match command {
        GatewayCommand::Submit => {
            let gateway = SubmitGateway::open(&settings)
                .map_err(|error| GatewayCommandError::Gateway(Box::new(error)))?;
            let response = gateway
                .submit(client_id, input)
                .map_err(|error| GatewayCommandError::Gateway(Box::new(error)))?;
            serde_json::to_writer(&mut output, &response).map_err(GatewayCommandError::Json)?;
            output.write_all(b"\n").map_err(GatewayCommandError::Io)
        }
        GatewayCommand::List => {
            let request = decode_control_request(input)?;
            let deadline = read_deadline(&settings);
            let gateway =
                ReadGateway::open_until(&settings, Some(deadline)).map_err(gateway_error)?;
            let encoded = gateway
                .list_encoded_until(&request, false, deadline)
                .map_err(gateway_error)?;
            write_encoded_response_until(output, encoded, deadline)
        }
        GatewayCommand::Recent => {
            let request = decode_control_request(input)?;
            let deadline = read_deadline(&settings);
            let gateway =
                ReadGateway::open_until(&settings, Some(deadline)).map_err(gateway_error)?;
            let encoded = gateway
                .list_encoded_until(&request, true, deadline)
                .map_err(gateway_error)?;
            write_encoded_response_until(output, encoded, deadline)
        }
        GatewayCommand::Get => {
            let request = decode_control_request(input)?;
            let deadline = read_deadline(&settings);
            let gateway =
                ReadGateway::open_until(&settings, Some(deadline)).map_err(gateway_error)?;
            let encoded = gateway
                .get_encoded_until(request, deadline)
                .map_err(gateway_error)?;
            write_encoded_response_until(output, encoded, deadline)
        }
        GatewayCommand::Export => {
            let request = decode_control_request(input)?;
            let deadline = read_deadline(&settings);
            let gateway =
                ReadGateway::open_until(&settings, Some(deadline)).map_err(gateway_error)?;
            let encoded = gateway
                .export_encoded_until(request, deadline)
                .map_err(gateway_error)?;
            write_encoded_response_until(output, encoded, deadline)
        }
        GatewayCommand::Search => {
            let request = decode_control_request(input)?;
            let deadline = read_deadline(&settings);
            let gateway =
                ReadGateway::open_until(&settings, Some(deadline)).map_err(gateway_error)?;
            let encoded = gateway
                .search_encoded_until(&request, deadline)
                .map_err(gateway_error)?;
            write_encoded_response_until(output, encoded, deadline)
        }
        GatewayCommand::Status => {
            let request = decode_control_request(input)?;
            let deadline = read_deadline(&settings);
            let gateway =
                StatusGateway::open_until(&settings, Some(deadline)).map_err(gateway_error)?;
            let encoded = gateway
                .status_encoded_until(request, deadline)
                .map_err(gateway_error)?;
            write_encoded_response_until(output, encoded, deadline)
        }
    }
}

fn read_deadline(settings: &GatewaySettings) -> Instant {
    Instant::now() + settings.read_operation_timeout()
}

fn decode_control_request<T: DeserializeOwned>(
    mut input: impl Read,
) -> Result<T, GatewayCommandError> {
    let mut bytes = Vec::new();
    input
        .by_ref()
        .take(MAXIMUM_CONTROL_REQUEST_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(GatewayCommandError::ControlInput)?;
    if bytes.len() as u64 > MAXIMUM_CONTROL_REQUEST_BYTES {
        return Err(GatewayCommandError::ControlRequestTooLarge);
    }
    serde_json::from_slice(&bytes).map_err(GatewayCommandError::ControlJson)
}

fn write_encoded_response_until<W>(
    output: W,
    response: Vec<u8>,
    deadline: Instant,
) -> Result<(), GatewayCommandError>
where
    W: AsFd,
{
    use nix::errno::Errno;
    use nix::fcntl::{FcntlArg, OFlag, fcntl};
    use nix::poll::{PollFd, PollFlags, PollTimeout, poll};

    let original_flags =
        OFlag::from_bits_truncate(fcntl(&output, FcntlArg::F_GETFL).map_err(|error| {
            GatewayCommandError::Io(io::Error::from_raw_os_error(error as i32))
        })?);
    fcntl(
        &output,
        FcntlArg::F_SETFL(original_flags | OFlag::O_NONBLOCK),
    )
    .map_err(|error| GatewayCommandError::Io(io::Error::from_raw_os_error(error as i32)))?;
    let _flags = OutputFlagGuard {
        output: &output,
        original_flags,
    };
    let mut written = 0_usize;
    while written < response.len() {
        let now = Instant::now();
        if now >= deadline {
            return Err(GatewayCommandError::OutputDeadline);
        }
        let timeout = PollTimeout::try_from(deadline.saturating_duration_since(now))
            .unwrap_or(PollTimeout::MAX);
        let mut descriptors = [PollFd::new(output.as_fd(), PollFlags::POLLOUT)];
        match poll(&mut descriptors, timeout) {
            Ok(0) => return Err(GatewayCommandError::OutputDeadline),
            Ok(_) => match nix::unistd::write(&output, &response[written..]) {
                Ok(0) => {
                    return Err(GatewayCommandError::Io(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "Gateway response output stopped accepting bytes",
                    )));
                }
                Ok(count) => written += count,
                Err(Errno::EAGAIN) => {}
                Err(Errno::EINTR) => {}
                Err(error) => {
                    return Err(GatewayCommandError::Io(io::Error::from_raw_os_error(
                        error as i32,
                    )));
                }
            },
            Err(Errno::EINTR) => {}
            Err(error) => {
                return Err(GatewayCommandError::Io(io::Error::from_raw_os_error(
                    error as i32,
                )));
            }
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
struct OutputFlagGuard<'a, W: AsFd> {
    output: &'a W,
    original_flags: nix::fcntl::OFlag,
}

#[cfg(target_os = "linux")]
impl<W> Drop for OutputFlagGuard<'_, W>
where
    W: AsFd,
{
    fn drop(&mut self) {
        let _ = nix::fcntl::fcntl(
            self.output,
            nix::fcntl::FcntlArg::F_SETFL(self.original_flags),
        );
    }
}

fn gateway_error(error: GatewayError) -> GatewayCommandError {
    GatewayCommandError::Gateway(Box::new(error))
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
                    "Gateway input deadline expired",
                ));
            }
            let remaining = self.deadline.saturating_duration_since(now);
            let timeout = PollTimeout::try_from(remaining).unwrap_or(PollTimeout::MAX);
            let mut descriptors = [PollFd::new(self.inner.as_fd(), PollFlags::POLLIN)];
            match poll(&mut descriptors, timeout) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "Gateway input deadline expired",
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
    InputSetup(io::Error),
    OutputSetup(io::Error),
    ControlInput(io::Error),
    ControlRequestTooLarge,
    ControlJson(serde_json::Error),
    Json(serde_json::Error),
    Io(io::Error),
    OutputDeadline,
}

impl GatewayCommandError {
    #[must_use]
    pub fn error_code(&self) -> ErrorCode {
        match self {
            Self::MissingCommand | Self::InvalidCommand | Self::ControlJson(_) => {
                ErrorCode::InvalidProtocol
            }
            Self::ControlRequestTooLarge => ErrorCode::LimitExceeded,
            Self::ControlInput(error) if error.kind() == io::ErrorKind::TimedOut => {
                ErrorCode::TemporaryFailure
            }
            Self::ControlInput(error) if is_peer_disconnect(error.kind()) => {
                ErrorCode::TemporaryFailure
            }
            Self::Json(error) if error.io_error_kind().is_some_and(is_peer_disconnect) => {
                ErrorCode::TemporaryFailure
            }
            Self::Io(error) if is_peer_disconnect(error.kind()) => ErrorCode::TemporaryFailure,
            Self::OutputDeadline => ErrorCode::TemporaryFailure,
            Self::Gateway(error) => error.error_code(),
            Self::InvalidClientIdEncoding
            | Self::ClientId(_)
            | Self::Config(_)
            | Self::InputSetup(_)
            | Self::OutputSetup(_)
            | Self::ControlInput(_)
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

fn is_peer_disconnect(kind: io::ErrorKind) -> bool {
    matches!(
        kind,
        io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::NotConnected
            | io::ErrorKind::UnexpectedEof
    )
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
            Self::InputSetup(error) => write!(formatter, "Gateway input setup failed: {error}"),
            Self::OutputSetup(error) => write!(formatter, "Gateway output setup failed: {error}"),
            Self::ControlInput(error) => write!(formatter, "Gateway control input failed: {error}"),
            Self::ControlRequestTooLarge => write!(
                formatter,
                "Gateway control request exceeds {MAXIMUM_CONTROL_REQUEST_BYTES} bytes"
            ),
            Self::ControlJson(error) => write!(formatter, "invalid Gateway control JSON: {error}"),
            Self::Json(error) => write!(formatter, "Gateway JSON encoding failed: {error}"),
            Self::Io(error) => write!(formatter, "Gateway response output failed: {error}"),
            Self::OutputDeadline => formatter.write_str("Gateway response deadline expired"),
        }
    }
}

impl std::error::Error for GatewayCommandError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ClientId(error) => Some(error),
            Self::Config(error) => Some(error.as_ref()),
            Self::Gateway(error) => Some(error.as_ref()),
            Self::InputSetup(error) => Some(error),
            Self::OutputSetup(error) => Some(error),
            Self::ControlInput(error) => Some(error),
            Self::ControlJson(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::Io(error) => Some(error),
            Self::MissingCommand
            | Self::InvalidCommand
            | Self::InvalidClientIdEncoding
            | Self::ControlRequestTooLarge
            | Self::OutputDeadline => None,
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
    use std::time::{Duration, Instant};

    use agent_knowledge_core::ErrorCode;

    use super::GatewayCommandError;
    #[cfg(target_os = "linux")]
    use super::{DeadlineReader, write_encoded_response_until};

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
        assert_eq!(
            GatewayCommandError::ControlInput(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "fictional timeout"
            ))
            .error_code(),
            ErrorCode::TemporaryFailure
        );
        assert!(
            GatewayCommandError::MissingCommand
                .to_string()
                .contains("SSH_ORIGINAL_COMMAND")
        );
        assert!(OsStr::new("fictional-node-a").to_str().is_some());
    }

    #[test]
    fn peer_disconnects_are_retryable_transport_failures() {
        for kind in [
            std::io::ErrorKind::BrokenPipe,
            std::io::ErrorKind::ConnectionAborted,
            std::io::ErrorKind::ConnectionReset,
            std::io::ErrorKind::NotConnected,
            std::io::ErrorKind::UnexpectedEof,
        ] {
            assert_eq!(
                GatewayCommandError::Io(std::io::Error::new(kind, "fictional peer disconnect"))
                    .error_code(),
                ErrorCode::TemporaryFailure
            );
            assert_eq!(
                GatewayCommandError::ControlInput(std::io::Error::new(
                    kind,
                    "fictional peer disconnect"
                ))
                .error_code(),
                ErrorCode::TemporaryFailure
            );
        }

        struct DisconnectedWriter;

        impl std::io::Write for DisconnectedWriter {
            fn write(&mut self, _buffer: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "fictional peer disconnect",
                ))
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let json_error = match serde_json::to_writer(DisconnectedWriter, &"fictional response") {
            Ok(()) => panic!("disconnected JSON output must fail"),
            Err(error) => error,
        };
        assert_eq!(
            GatewayCommandError::Json(json_error).error_code(),
            ErrorCode::TemporaryFailure
        );
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

    #[cfg(target_os = "linux")]
    #[test]
    fn committed_response_timeout_closes_the_output_without_a_writer_thread() {
        let (writer, mut reader) = UnixStream::pair()
            .unwrap_or_else(|error| panic!("response stream pair must open: {error}"));
        let response = vec![b'x'; 8 * 1024 * 1024];
        let response_length = response.len();
        let started = Instant::now();
        assert!(matches!(
            write_encoded_response_until(writer, response, started + Duration::from_millis(25),),
            Err(GatewayCommandError::OutputDeadline)
        ));
        assert!(started.elapsed() < Duration::from_millis(500));
        let mut delivered = Vec::new();
        reader
            .read_to_end(&mut delivered)
            .unwrap_or_else(|error| panic!("closed response stream must drain: {error}"));
        assert!(delivered.len() < response_length);
    }
}

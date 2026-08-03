use std::env;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::str::FromStr;

use agent_knowledge_protocol::{ClientId, ClientIdError};

#[cfg(unix)]
use std::os::unix::process::CommandExt;

const FORCED_COMMAND_VERSION: &str = "akg-v1";
const GATEWAY_PROGRAM_NAME: &str = "agent-knowledge";

fn main() -> ExitCode {
    match launch(env::args_os()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("could not start forced-command Gateway: {error}");
            ExitCode::from(126)
        }
    }
}

fn launch(arguments: impl IntoIterator<Item = OsString>) -> Result<(), ShellError> {
    let invocation = ForcedCommandInvocation::parse(arguments)?;
    let current_executable = env::current_exe().map_err(ShellError::CurrentExecutable)?;
    let gateway_program = sibling_gateway_program(&current_executable)?;
    let mut command = Command::new(gateway_program);
    command
        .arg("gateway")
        .arg("--config")
        .arg(invocation.config)
        .arg("--client-id")
        .arg(invocation.client_id.as_str());

    #[cfg(unix)]
    {
        Err(ShellError::Execute(command.exec()))
    }
    #[cfg(not(unix))]
    {
        let _ = command;
        Err(ShellError::UnsupportedPlatform)
    }
}

#[derive(Debug, Eq, PartialEq)]
struct ForcedCommandInvocation {
    config: PathBuf,
    client_id: ClientId,
}

impl ForcedCommandInvocation {
    fn parse(arguments: impl IntoIterator<Item = OsString>) -> Result<Self, ShellError> {
        let mut arguments = arguments.into_iter();
        let _program = arguments.next().ok_or(ShellError::InvalidInvocation)?;
        if arguments.next().as_deref() != Some(OsStr::new("-c")) {
            return Err(ShellError::InvalidInvocation);
        }
        let forced_command = arguments.next().ok_or(ShellError::InvalidInvocation)?;
        if arguments.next().is_some() {
            return Err(ShellError::InvalidInvocation);
        }
        let forced_command = forced_command
            .to_str()
            .ok_or(ShellError::NonUtf8ForcedCommand)?;
        let mut fields = forced_command.split(' ');
        if fields.next() != Some(FORCED_COMMAND_VERSION) {
            return Err(ShellError::InvalidForcedCommand);
        }
        let config = fields.next().ok_or(ShellError::InvalidForcedCommand)?;
        let client_id = fields.next().ok_or(ShellError::InvalidForcedCommand)?;
        if fields.next().is_some() {
            return Err(ShellError::InvalidForcedCommand);
        }
        let config = PathBuf::from(config);
        if !config.is_absolute() {
            return Err(ShellError::RelativeConfigPath);
        }
        let client_id = ClientId::from_str(client_id).map_err(ShellError::ClientId)?;
        Ok(Self { config, client_id })
    }
}

fn sibling_gateway_program(current_executable: &Path) -> Result<PathBuf, ShellError> {
    let directory = current_executable
        .parent()
        .ok_or(ShellError::MissingExecutableDirectory)?;
    Ok(directory.join(GATEWAY_PROGRAM_NAME))
}

#[derive(Debug)]
enum ShellError {
    InvalidInvocation,
    NonUtf8ForcedCommand,
    InvalidForcedCommand,
    RelativeConfigPath,
    ClientId(ClientIdError),
    CurrentExecutable(std::io::Error),
    MissingExecutableDirectory,
    Execute(std::io::Error),
    #[cfg(not(unix))]
    UnsupportedPlatform,
}

impl fmt::Display for ShellError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInvocation => formatter.write_str("invalid login-shell invocation"),
            Self::NonUtf8ForcedCommand => formatter.write_str("forced command is not valid UTF-8"),
            Self::InvalidForcedCommand => {
                formatter.write_str("forced command does not match the supported grammar")
            }
            Self::RelativeConfigPath => {
                formatter.write_str("Gateway configuration path must be absolute")
            }
            Self::ClientId(error) => write!(formatter, "invalid authenticated client ID: {error}"),
            Self::CurrentExecutable(error) => {
                write!(
                    formatter,
                    "could not resolve the login-shell executable: {error}"
                )
            }
            Self::MissingExecutableDirectory => {
                formatter.write_str("login-shell executable has no parent directory")
            }
            Self::Execute(error) => write!(formatter, "could not execute the Gateway: {error}"),
            #[cfg(not(unix))]
            Self::UnsupportedPlatform => formatter.write_str("platform is not supported"),
        }
    }
}

impl std::error::Error for ShellError {}

#[cfg(test)]
mod tests {
    use super::{ForcedCommandInvocation, ShellError, sibling_gateway_program};
    use std::ffi::OsString;
    use std::path::{Path, PathBuf};

    fn arguments(command: &str) -> Vec<OsString> {
        vec![
            "agent-knowledge-ssh-shell".into(),
            "-c".into(),
            command.into(),
        ]
    }

    #[test]
    fn parses_the_root_controlled_forced_command() {
        let invocation = ForcedCommandInvocation::parse(arguments(
            "akg-v1 /etc/agent-knowledge/gateway.yaml fictional-node-a",
        ))
        .unwrap_or_else(|error| panic!("forced command must parse: {error}"));

        assert_eq!(
            invocation.config,
            Path::new("/etc/agent-knowledge/gateway.yaml")
        );
        assert_eq!(invocation.client_id.as_str(), "fictional-node-a");
    }

    #[test]
    fn rejects_shell_syntax_whitespace_and_extra_arguments() {
        for command in [
            "akg-v1 /etc/agent-knowledge/gateway.yaml fictional-node-a;id",
            "akg-v1  /etc/agent-knowledge/gateway.yaml fictional-node-a",
            "akg-v1 /etc/agent-knowledge/gateway.yaml fictional-node-a extra",
            "/bin/agent-knowledge gateway",
        ] {
            assert!(ForcedCommandInvocation::parse(arguments(command)).is_err());
        }
        assert!(matches!(
            ForcedCommandInvocation::parse([
                "agent-knowledge-ssh-shell".into(),
                "-c".into(),
                "akg-v1 /etc/agent-knowledge/gateway.yaml fictional-node-a".into(),
                "extra".into(),
            ]),
            Err(ShellError::InvalidInvocation)
        ));
    }

    #[test]
    fn rejects_relative_configuration_and_invalid_client_identity() {
        assert!(matches!(
            ForcedCommandInvocation::parse(arguments(
                "akg-v1 etc/agent-knowledge/gateway.yaml fictional-node-a"
            )),
            Err(ShellError::RelativeConfigPath)
        ));
        assert!(matches!(
            ForcedCommandInvocation::parse(arguments(
                "akg-v1 /etc/agent-knowledge/gateway.yaml Fictional_Node"
            )),
            Err(ShellError::ClientId(_))
        ));
    }

    #[test]
    fn locates_the_gateway_beside_the_login_shell() {
        assert_eq!(
            sibling_gateway_program(Path::new("/opt/fictional/bin/agent-knowledge-ssh-shell"))
                .unwrap_or_else(|error| panic!("sibling path must resolve: {error}")),
            PathBuf::from("/opt/fictional/bin/agent-knowledge")
        );
    }
}

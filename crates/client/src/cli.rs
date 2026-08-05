use std::ffi::OsString;
use std::fmt;
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::Duration;

use agent_knowledge_protocol::{
    ExportRequest, GetRequest, LIST_COMMAND, ListRequest, ListResponse, RECENT_COMMAND,
    ReadFilterRequest, SEARCH_COMMAND, SearchRequest, StatusRequest,
};

use crate::ClientCommandError;

const USAGE: &str = "usage:\n\
    agent-knowledge-client --version\n\
    agent-knowledge-client mcp --destination <ssh-destination> [--timeout-seconds <seconds>]\n\
    agent-knowledge-client submit --destination <ssh-destination> --package-root <path> [--timeout-seconds <seconds>]\n\
    agent-knowledge-client list --destination <ssh-destination> [--project <id>] [--tag <tag>] [--session <id>] [--include-archived] [--maximum-results <count>] [--timeout-seconds <seconds>]\n\
    agent-knowledge-client recent --destination <ssh-destination> [--project <id>] [--tag <tag>] [--session <id>] [--include-archived] [--maximum-results <count>] [--timeout-seconds <seconds>]\n\
    agent-knowledge-client get --destination <ssh-destination> --document-id <id> [--timeout-seconds <seconds>]\n\
    agent-knowledge-client export --destination <ssh-destination> --document-id <id> [--timeout-seconds <seconds>]\n\
    agent-knowledge-client status --destination <ssh-destination> --request-id <id> [--timeout-seconds <seconds>]\n\
    agent-knowledge-client search --destination <ssh-destination> --query <text> [--project <id>] [--tag <tag>] [--session <id>] [--include-archived] [--maximum-results <count>] [--timeout-seconds <seconds>]";
const DEFAULT_TIMEOUT_SECONDS: u64 = 300;
const MAXIMUM_TIMEOUT_SECONDS: u64 = 3_600;
const DEFAULT_READ_RESULTS: usize = 100;
const MAXIMUM_READ_RESULTS: usize = 10_000;

#[derive(Debug)]
pub enum Command {
    Version,
    Mcp {
        destination: OsString,
        timeout: Duration,
    },
    Submit {
        destination: OsString,
        package_root: PathBuf,
        timeout: Duration,
    },
    #[doc(hidden)]
    InternalMcpSubmit {
        destination: OsString,
        package_root: PathBuf,
        timeout: Duration,
    },
    List {
        destination: OsString,
        request: ListRequest,
        recent: bool,
        timeout: Duration,
    },
    Get {
        destination: OsString,
        request: GetRequest,
        timeout: Duration,
    },
    Export {
        destination: OsString,
        request: ExportRequest,
        timeout: Duration,
    },
    Search {
        destination: OsString,
        request: SearchRequest,
        timeout: Duration,
    },
    Status {
        destination: OsString,
        request: StatusRequest,
        timeout: Duration,
    },
}

#[derive(Debug)]
pub struct ParseError;

pub fn run<I, W>(arguments: I, output: W) -> Result<(), CliError>
where
    I: IntoIterator<Item = OsString>,
    W: Write,
{
    let command = parse_arguments(arguments).map_err(|_| CliError::Usage)?;
    execute(command, output)
}

pub fn execute<W>(command: Command, mut output: W) -> Result<(), CliError>
where
    W: Write,
{
    match command {
        Command::Version => writeln!(
            output,
            "agent-knowledge-client {}",
            env!("CARGO_PKG_VERSION")
        )
        .map_err(CliError::Output),
        Command::Mcp {
            destination,
            timeout,
        } => {
            let client = crate::SshClient::new(destination, timeout).map_err(CliError::Command)?;
            crate::mcp::run(client).map_err(CliError::Mcp)
        }
        Command::Submit {
            destination,
            package_root,
            timeout,
        } => crate::submit(&destination, &package_root, timeout, output).map_err(CliError::Command),
        Command::InternalMcpSubmit {
            destination,
            package_root,
            timeout,
        } => crate::submit_inherited_process_group(&destination, &package_root, timeout, output)
            .map_err(CliError::Command),
        Command::List {
            destination,
            request,
            recent,
            timeout,
        } => crate::control::<_, ListResponse>(
            &destination,
            if recent { RECENT_COMMAND } else { LIST_COMMAND },
            &request,
            timeout,
            output,
        )
        .map_err(CliError::Command),
        Command::Get {
            destination,
            request,
            timeout,
        } => crate::get(&destination, &request, timeout, output).map_err(CliError::Command),
        Command::Export {
            destination,
            request,
            timeout,
        } => crate::export(&destination, &request, timeout, output).map_err(CliError::Command),
        Command::Search {
            destination,
            request,
            timeout,
        } => crate::control::<_, ListResponse>(
            &destination,
            SEARCH_COMMAND,
            &request,
            timeout,
            output,
        )
        .map_err(CliError::Command),
        Command::Status {
            destination,
            request,
            timeout,
        } => crate::status(&destination, &request, timeout, output).map_err(CliError::Command),
    }
}

pub fn parse_arguments<I>(arguments: I) -> Result<Command, ParseError>
where
    I: IntoIterator<Item = OsString>,
{
    let mut arguments = arguments.into_iter();
    let action = arguments.next().ok_or(ParseError)?;
    if action == "--version" {
        return if arguments.next().is_none() {
            Ok(Command::Version)
        } else {
            Err(ParseError)
        };
    }
    match action.to_str() {
        Some("submit") => parse_submit_arguments(arguments),
        Some("__mcp-submit") => parse_internal_mcp_submit_arguments(arguments),
        Some("mcp") => parse_mcp_arguments(arguments),
        Some("list") => parse_list_arguments(arguments, false),
        Some("recent") => parse_list_arguments(arguments, true),
        Some("get") => parse_document_arguments(arguments, false),
        Some("export") => parse_document_arguments(arguments, true),
        Some("search") => parse_search_arguments(arguments),
        Some("status") => parse_status_arguments(arguments),
        _ => Err(ParseError),
    }
}

fn parse_internal_mcp_submit_arguments<I>(mut arguments: I) -> Result<Command, ParseError>
where
    I: Iterator<Item = OsString>,
{
    let mut destination = None;
    let mut package_root = None;
    let mut timeout_milliseconds = None;
    while let Some(flag) = arguments.next() {
        let value = arguments.next().ok_or(ParseError)?;
        match flag.to_str() {
            Some("--destination") if destination.is_none() => destination = Some(value),
            Some("--package-root") if package_root.is_none() => {
                package_root = Some(PathBuf::from(value));
            }
            Some("--timeout-milliseconds") if timeout_milliseconds.is_none() => {
                timeout_milliseconds = Some(
                    value
                        .to_str()
                        .and_then(|value| value.parse::<u64>().ok())
                        .filter(|value| *value > 0)
                        .ok_or(ParseError)?,
                );
            }
            _ => return Err(ParseError),
        }
    }
    Ok(Command::InternalMcpSubmit {
        destination: destination.ok_or(ParseError)?,
        package_root: package_root.ok_or(ParseError)?,
        timeout: Duration::from_millis(timeout_milliseconds.ok_or(ParseError)?),
    })
}

fn parse_mcp_arguments<I>(mut arguments: I) -> Result<Command, ParseError>
where
    I: Iterator<Item = OsString>,
{
    let mut destination = None;
    let mut timeout_seconds = None;
    while let Some(flag) = arguments.next() {
        let value = arguments.next().ok_or(ParseError)?;
        match flag.to_str() {
            Some("--destination") if destination.is_none() => destination = Some(value),
            Some("--timeout-seconds") if timeout_seconds.is_none() => {
                timeout_seconds = Some(parse_bounded_u64(&value, MAXIMUM_TIMEOUT_SECONDS)?);
            }
            _ => return Err(ParseError),
        }
    }
    Ok(Command::Mcp {
        destination: destination.ok_or(ParseError)?,
        timeout: Duration::from_secs(timeout_seconds.unwrap_or(DEFAULT_TIMEOUT_SECONDS)),
    })
}

fn parse_submit_arguments<I>(mut arguments: I) -> Result<Command, ParseError>
where
    I: Iterator<Item = OsString>,
{
    let mut destination = None;
    let mut package_root = None;
    let mut timeout_seconds = None;
    while let Some(flag) = arguments.next() {
        let value = arguments.next().ok_or(ParseError)?;
        match flag.to_str() {
            Some("--destination") if destination.is_none() => destination = Some(value),
            Some("--package-root") if package_root.is_none() => {
                package_root = Some(PathBuf::from(value));
            }
            Some("--timeout-seconds") if timeout_seconds.is_none() => {
                timeout_seconds = Some(parse_bounded_u64(&value, MAXIMUM_TIMEOUT_SECONDS)?);
            }
            _ => return Err(ParseError),
        }
    }
    Ok(Command::Submit {
        destination: destination.ok_or(ParseError)?,
        package_root: package_root.ok_or(ParseError)?,
        timeout: Duration::from_secs(timeout_seconds.unwrap_or(DEFAULT_TIMEOUT_SECONDS)),
    })
}

struct ParsedReadArguments {
    destination: OsString,
    filter: ReadFilterRequest,
    maximum_results: usize,
    query: Option<String>,
    timeout: Duration,
}

fn parse_list_arguments<I>(arguments: I, recent: bool) -> Result<Command, ParseError>
where
    I: Iterator<Item = OsString>,
{
    let parsed = parse_read_arguments(arguments, false)?;
    Ok(Command::List {
        destination: parsed.destination,
        request: ListRequest::new(parsed.filter, parsed.maximum_results),
        recent,
        timeout: parsed.timeout,
    })
}

fn parse_search_arguments<I>(arguments: I) -> Result<Command, ParseError>
where
    I: Iterator<Item = OsString>,
{
    let parsed = parse_read_arguments(arguments, true)?;
    Ok(Command::Search {
        destination: parsed.destination,
        request: SearchRequest::new(
            parsed.query.ok_or(ParseError)?,
            parsed.filter,
            parsed.maximum_results,
        ),
        timeout: parsed.timeout,
    })
}

fn parse_read_arguments<I>(
    mut arguments: I,
    allow_query: bool,
) -> Result<ParsedReadArguments, ParseError>
where
    I: Iterator<Item = OsString>,
{
    let mut destination = None;
    let mut project = None;
    let mut tag = None;
    let mut session = None;
    let mut include_archived = false;
    let mut maximum_results = None;
    let mut query = None;
    let mut timeout_seconds = None;
    while let Some(flag) = arguments.next() {
        if flag == "--include-archived" {
            if include_archived {
                return Err(ParseError);
            }
            include_archived = true;
            continue;
        }
        let value = arguments.next().ok_or(ParseError)?;
        match flag.to_str() {
            Some("--destination") if destination.is_none() => destination = Some(value),
            Some("--project") if project.is_none() => {
                project = Some(
                    value
                        .to_str()
                        .and_then(|value| value.parse().ok())
                        .ok_or(ParseError)?,
                );
            }
            Some("--tag") if tag.is_none() => {
                tag = Some(value.into_string().map_err(|_| ParseError)?);
            }
            Some("--session") if session.is_none() => {
                session = Some(
                    value
                        .to_str()
                        .and_then(|value| value.parse().ok())
                        .ok_or(ParseError)?,
                );
            }
            Some("--maximum-results") if maximum_results.is_none() => {
                maximum_results = Some(parse_bounded_usize(&value, MAXIMUM_READ_RESULTS)?);
            }
            Some("--query") if allow_query && query.is_none() => {
                query = Some(value.into_string().map_err(|_| ParseError)?);
            }
            Some("--timeout-seconds") if timeout_seconds.is_none() => {
                timeout_seconds = Some(parse_bounded_u64(&value, MAXIMUM_TIMEOUT_SECONDS)?);
            }
            _ => return Err(ParseError),
        }
    }
    Ok(ParsedReadArguments {
        destination: destination.ok_or(ParseError)?,
        filter: ReadFilterRequest {
            project,
            tag,
            session,
            include_archived,
        },
        maximum_results: maximum_results.unwrap_or(DEFAULT_READ_RESULTS),
        query,
        timeout: Duration::from_secs(timeout_seconds.unwrap_or(DEFAULT_TIMEOUT_SECONDS)),
    })
}

fn parse_document_arguments<I>(mut arguments: I, export: bool) -> Result<Command, ParseError>
where
    I: Iterator<Item = OsString>,
{
    let mut destination = None;
    let mut document_id = None;
    let mut timeout_seconds = None;
    while let Some(flag) = arguments.next() {
        let value = arguments.next().ok_or(ParseError)?;
        match flag.to_str() {
            Some("--destination") if destination.is_none() => destination = Some(value),
            Some("--document-id") if document_id.is_none() => {
                document_id = Some(
                    value
                        .to_str()
                        .and_then(|value| value.parse().ok())
                        .ok_or(ParseError)?,
                );
            }
            Some("--timeout-seconds") if timeout_seconds.is_none() => {
                timeout_seconds = Some(parse_bounded_u64(&value, MAXIMUM_TIMEOUT_SECONDS)?);
            }
            _ => return Err(ParseError),
        }
    }
    let destination = destination.ok_or(ParseError)?;
    let document_id = document_id.ok_or(ParseError)?;
    let timeout = Duration::from_secs(timeout_seconds.unwrap_or(DEFAULT_TIMEOUT_SECONDS));
    if export {
        Ok(Command::Export {
            destination,
            request: ExportRequest::new(document_id),
            timeout,
        })
    } else {
        Ok(Command::Get {
            destination,
            request: GetRequest::new(document_id),
            timeout,
        })
    }
}

fn parse_status_arguments<I>(mut arguments: I) -> Result<Command, ParseError>
where
    I: Iterator<Item = OsString>,
{
    let mut destination = None;
    let mut request_id = None;
    let mut timeout_seconds = None;
    while let Some(flag) = arguments.next() {
        let value = arguments.next().ok_or(ParseError)?;
        match flag.to_str() {
            Some("--destination") if destination.is_none() => destination = Some(value),
            Some("--request-id") if request_id.is_none() => {
                request_id = Some(
                    value
                        .to_str()
                        .and_then(|value| value.parse().ok())
                        .ok_or(ParseError)?,
                );
            }
            Some("--timeout-seconds") if timeout_seconds.is_none() => {
                timeout_seconds = Some(parse_bounded_u64(&value, MAXIMUM_TIMEOUT_SECONDS)?);
            }
            _ => return Err(ParseError),
        }
    }
    Ok(Command::Status {
        destination: destination.ok_or(ParseError)?,
        request: StatusRequest::new(request_id.ok_or(ParseError)?),
        timeout: Duration::from_secs(timeout_seconds.unwrap_or(DEFAULT_TIMEOUT_SECONDS)),
    })
}

fn parse_bounded_u64(value: &OsString, maximum: u64) -> Result<u64, ParseError> {
    value
        .to_str()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| (1..=maximum).contains(value))
        .ok_or(ParseError)
}

fn parse_bounded_usize(value: &OsString, maximum: usize) -> Result<usize, ParseError> {
    value
        .to_str()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| (1..=maximum).contains(value))
        .ok_or(ParseError)
}

#[derive(Debug)]
pub enum CliError {
    Usage,
    Command(ClientCommandError),
    Mcp(crate::mcp::McpServerError),
    Output(io::Error),
}

impl CliError {
    pub fn write_diagnostic(&self, mut output: impl Write) -> io::Result<()> {
        match self {
            Self::Command(error) => error.write_diagnostic(output),
            _ => writeln!(output, "{self}"),
        }
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage => formatter.write_str(USAGE),
            Self::Command(error) => error.fmt(formatter),
            Self::Mcp(error) => error.fmt(formatter),
            Self::Output(error) => write!(formatter, "could not write command output: {error}"),
        }
    }
}

impl std::error::Error for CliError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Command(error) => Some(error),
            Self::Mcp(error) => Some(error),
            Self::Output(error) => Some(error),
            Self::Usage => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CliError, Command, parse_arguments, run};
    use std::ffi::OsString;
    use std::path::Path;
    use std::time::Duration;

    #[test]
    fn reports_the_client_version() {
        let mut output = Vec::new();
        run([OsString::from("--version")], &mut output)
            .unwrap_or_else(|error| panic!("version command must succeed: {error}"));
        assert_eq!(
            String::from_utf8(output).ok().as_deref(),
            Some(concat!(
                "agent-knowledge-client ",
                env!("CARGO_PKG_VERSION"),
                "\n"
            ))
        );
    }

    #[test]
    fn parses_client_commands_and_bounds_options() {
        let submit = parse_arguments([
            "submit".into(),
            "--destination".into(),
            "fictional-knowledge".into(),
            "--package-root".into(),
            "/tmp/fictional-package".into(),
            "--timeout-seconds".into(),
            "42".into(),
        ])
        .unwrap_or_else(|_| panic!("client submit command must parse"));
        assert!(matches!(
            submit,
            Command::Submit { destination, package_root, timeout }
                if destination == "fictional-knowledge"
                    && package_root == Path::new("/tmp/fictional-package")
                    && timeout == Duration::from_secs(42)
        ));

        let mcp = parse_arguments([
            "mcp".into(),
            "--destination".into(),
            "fictional-knowledge".into(),
            "--timeout-seconds".into(),
            "60".into(),
        ])
        .unwrap_or_else(|_| panic!("client MCP command must parse"));
        assert!(matches!(
            mcp,
            Command::Mcp { destination, timeout }
                if destination == "fictional-knowledge"
                    && timeout == Duration::from_secs(60)
        ));

        let recent = parse_arguments([
            "recent".into(),
            "--destination".into(),
            "fictional-knowledge".into(),
            "--project".into(),
            "fictional-project".into(),
            "--tag".into(),
            "operations".into(),
            "--include-archived".into(),
            "--maximum-results".into(),
            "25".into(),
        ])
        .unwrap_or_else(|_| panic!("client recent command must parse"));
        assert!(matches!(
            recent,
            Command::List { destination, request, recent: true, timeout }
                if destination == "fictional-knowledge"
                    && request.filter.project.is_some()
                    && request.filter.tag.as_deref() == Some("operations")
                    && request.filter.include_archived
                    && request.maximum_results == 25
                    && timeout == Duration::from_secs(300)
        ));

        for arguments in [
            vec!["--version".into(), "extra".into()],
            vec![
                "search".into(),
                "--destination".into(),
                "fictional-knowledge".into(),
            ],
            vec![
                "submit".into(),
                "--destination".into(),
                "fictional-knowledge".into(),
                "--package-root".into(),
                "/tmp/fictional-package".into(),
                "--timeout-seconds".into(),
                "3601".into(),
            ],
        ] {
            assert!(parse_arguments(arguments).is_err());
        }
    }

    #[test]
    fn writes_usage_for_invalid_commands() {
        let error = match run(Vec::<OsString>::new(), Vec::new()) {
            Ok(()) => panic!("an empty command line must fail"),
            Err(error) => error,
        };
        assert!(matches!(error, CliError::Usage));
        assert!(error.to_string().starts_with("usage:"));
    }
}

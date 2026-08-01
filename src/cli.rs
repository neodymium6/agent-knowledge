use std::ffi::OsString;
use std::fmt;
use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use agent_knowledge_protocol::{
    GetRequest, LIST_COMMAND, ListRequest, ListResponse, RECENT_COMMAND, ReadFilterRequest,
    SEARCH_COMMAND, SearchRequest,
};
use agent_knowledge_queue::{
    EnqueueOutcome, FileQueue, PackagePolicy, PackageValidationError, QueueError, validate_package,
};
use serde::Serialize;

use crate::client::{self, ClientCommandError};
use crate::gateway::{self, GatewayCommandError};
use crate::worker::{self, WorkerCommandError};

const USAGE: &str = "usage:\n\
    agent-knowledge admin submit --queue-root <path> --package-root <path>\n\
    agent-knowledge client submit --destination <ssh-destination> --package-root <path> [--timeout-seconds <seconds>]\n\
    agent-knowledge client list --destination <ssh-destination> [--project <id>] [--tag <tag>] [--session <id>] [--include-archived] [--maximum-results <count>] [--timeout-seconds <seconds>]\n\
    agent-knowledge client recent --destination <ssh-destination> [--project <id>] [--tag <tag>] [--session <id>] [--include-archived] [--maximum-results <count>] [--timeout-seconds <seconds>]\n\
    agent-knowledge client get --destination <ssh-destination> --document-id <id> [--timeout-seconds <seconds>]\n\
    agent-knowledge client search --destination <ssh-destination> --query <text> [--project <id>] [--tag <tag>] [--session <id>] [--include-archived] [--maximum-results <count>] [--timeout-seconds <seconds>]\n\
    agent-knowledge gateway --config <path> --client-id <id>\n\
    agent-knowledge worker run --config <path>";
const DEFAULT_CLIENT_TIMEOUT_SECONDS: u64 = 300;
const MAXIMUM_CLIENT_TIMEOUT_SECONDS: u64 = 3_600;
const DEFAULT_READ_RESULTS: usize = 100;
const MAXIMUM_READ_RESULTS: usize = 10_000;

pub fn run<I, W>(arguments: I, output: W) -> Result<(), CliError>
where
    I: IntoIterator<Item = OsString>,
    W: Write,
{
    match parse_arguments(arguments)? {
        Command::Submit {
            queue_root,
            package_root,
        } => submit_directory(&queue_root, &package_root, output),
        Command::RunWorker { config } => worker::run(&config, output).map_err(CliError::Worker),
        Command::RunGateway { config, client_id } => gateway::run_stdio(
            &config,
            &client_id,
            std::env::var_os("SSH_ORIGINAL_COMMAND"),
        )
        .map_err(CliError::Gateway),
        Command::ClientSubmit {
            destination,
            package_root,
            timeout,
        } => client::submit(&destination, &package_root, timeout, output).map_err(CliError::Client),
        Command::ClientList {
            destination,
            request,
            recent,
            timeout,
        } => client::control::<_, ListResponse>(
            &destination,
            if recent { RECENT_COMMAND } else { LIST_COMMAND },
            &request,
            timeout,
            output,
        )
        .map_err(CliError::Client),
        Command::ClientGet {
            destination,
            request,
            timeout,
        } => client::get(&destination, &request, timeout, output).map_err(CliError::Client),
        Command::ClientSearch {
            destination,
            request,
            timeout,
        } => client::control::<_, ListResponse>(
            &destination,
            SEARCH_COMMAND,
            &request,
            timeout,
            output,
        )
        .map_err(CliError::Client),
    }
}

enum Command {
    Submit {
        queue_root: PathBuf,
        package_root: PathBuf,
    },
    RunWorker {
        config: PathBuf,
    },
    RunGateway {
        config: PathBuf,
        client_id: OsString,
    },
    ClientSubmit {
        destination: OsString,
        package_root: PathBuf,
        timeout: Duration,
    },
    ClientList {
        destination: OsString,
        request: ListRequest,
        recent: bool,
        timeout: Duration,
    },
    ClientGet {
        destination: OsString,
        request: GetRequest,
        timeout: Duration,
    },
    ClientSearch {
        destination: OsString,
        request: SearchRequest,
        timeout: Duration,
    },
}

fn parse_arguments<I>(arguments: I) -> Result<Command, CliError>
where
    I: IntoIterator<Item = OsString>,
{
    let mut arguments = arguments.into_iter();
    let _program = arguments.next();
    let namespace = arguments.next();
    let action = arguments.next();
    if namespace.as_deref() == Some(std::ffi::OsStr::new("gateway")) {
        return parse_gateway_arguments(action.into_iter().chain(arguments));
    }
    match (namespace.as_deref(), action.as_deref()) {
        (Some(namespace), Some(action))
            if namespace == std::ffi::OsStr::new("client")
                && action == std::ffi::OsStr::new("submit") =>
        {
            parse_client_submit_arguments(arguments)
        }
        (Some(namespace), Some(action))
            if namespace == std::ffi::OsStr::new("client")
                && (action == std::ffi::OsStr::new("list")
                    || action == std::ffi::OsStr::new("recent")) =>
        {
            parse_client_list_arguments(arguments, action == std::ffi::OsStr::new("recent"))
        }
        (Some(namespace), Some(action))
            if namespace == std::ffi::OsStr::new("client")
                && action == std::ffi::OsStr::new("get") =>
        {
            parse_client_get_arguments(arguments)
        }
        (Some(namespace), Some(action))
            if namespace == std::ffi::OsStr::new("client")
                && action == std::ffi::OsStr::new("search") =>
        {
            parse_client_search_arguments(arguments)
        }
        (Some(namespace), Some(action))
            if namespace == std::ffi::OsStr::new("admin")
                && action == std::ffi::OsStr::new("submit") =>
        {
            parse_submit_arguments(arguments)
        }
        (Some(namespace), Some(action))
            if namespace == std::ffi::OsStr::new("worker")
                && action == std::ffi::OsStr::new("run") =>
        {
            parse_worker_arguments(arguments)
        }
        _ => Err(CliError::Usage),
    }
}

fn parse_client_submit_arguments<I>(mut arguments: I) -> Result<Command, CliError>
where
    I: Iterator<Item = OsString>,
{
    let mut destination = None;
    let mut package_root = None;
    let mut timeout_seconds = None;
    while let Some(flag) = arguments.next() {
        let value = arguments.next().ok_or(CliError::Usage)?;
        match flag.to_str() {
            Some("--destination") if destination.is_none() => destination = Some(value),
            Some("--package-root") if package_root.is_none() => {
                package_root = Some(PathBuf::from(value));
            }
            Some("--timeout-seconds") if timeout_seconds.is_none() => {
                let seconds = value
                    .to_str()
                    .and_then(|value| value.parse::<u64>().ok())
                    .filter(|seconds| (1..=MAXIMUM_CLIENT_TIMEOUT_SECONDS).contains(seconds))
                    .ok_or(CliError::Usage)?;
                timeout_seconds = Some(seconds);
            }
            _ => return Err(CliError::Usage),
        }
    }
    Ok(Command::ClientSubmit {
        destination: destination.ok_or(CliError::Usage)?,
        package_root: package_root.ok_or(CliError::Usage)?,
        timeout: Duration::from_secs(timeout_seconds.unwrap_or(DEFAULT_CLIENT_TIMEOUT_SECONDS)),
    })
}

struct ParsedReadArguments {
    destination: OsString,
    filter: ReadFilterRequest,
    maximum_results: usize,
    query: Option<String>,
    timeout: Duration,
}

fn parse_client_list_arguments<I>(arguments: I, recent: bool) -> Result<Command, CliError>
where
    I: Iterator<Item = OsString>,
{
    let parsed = parse_read_arguments(arguments, false)?;
    Ok(Command::ClientList {
        destination: parsed.destination,
        request: ListRequest::new(parsed.filter, parsed.maximum_results),
        recent,
        timeout: parsed.timeout,
    })
}

fn parse_client_search_arguments<I>(arguments: I) -> Result<Command, CliError>
where
    I: Iterator<Item = OsString>,
{
    let parsed = parse_read_arguments(arguments, true)?;
    Ok(Command::ClientSearch {
        destination: parsed.destination,
        request: SearchRequest::new(
            parsed.query.ok_or(CliError::Usage)?,
            parsed.filter,
            parsed.maximum_results,
        ),
        timeout: parsed.timeout,
    })
}

fn parse_read_arguments<I>(
    mut arguments: I,
    allow_query: bool,
) -> Result<ParsedReadArguments, CliError>
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
        if flag == std::ffi::OsStr::new("--include-archived") {
            if include_archived {
                return Err(CliError::Usage);
            }
            include_archived = true;
            continue;
        }
        let value = arguments.next().ok_or(CliError::Usage)?;
        match flag.to_str() {
            Some("--destination") if destination.is_none() => destination = Some(value),
            Some("--project") if project.is_none() => {
                project = Some(
                    value
                        .to_str()
                        .and_then(|value| value.parse().ok())
                        .ok_or(CliError::Usage)?,
                );
            }
            Some("--tag") if tag.is_none() => {
                tag = Some(value.into_string().map_err(|_| CliError::Usage)?);
            }
            Some("--session") if session.is_none() => {
                session = Some(
                    value
                        .to_str()
                        .and_then(|value| value.parse().ok())
                        .ok_or(CliError::Usage)?,
                );
            }
            Some("--maximum-results") if maximum_results.is_none() => {
                maximum_results = Some(parse_bounded_usize(&value, MAXIMUM_READ_RESULTS)?);
            }
            Some("--query") if allow_query && query.is_none() => {
                query = Some(value.into_string().map_err(|_| CliError::Usage)?);
            }
            Some("--timeout-seconds") if timeout_seconds.is_none() => {
                timeout_seconds = Some(parse_bounded_u64(&value, MAXIMUM_CLIENT_TIMEOUT_SECONDS)?);
            }
            _ => return Err(CliError::Usage),
        }
    }
    if !allow_query && query.is_some() {
        return Err(CliError::Usage);
    }
    Ok(ParsedReadArguments {
        destination: destination.ok_or(CliError::Usage)?,
        filter: ReadFilterRequest {
            project,
            tag,
            session,
            include_archived,
        },
        maximum_results: maximum_results.unwrap_or(DEFAULT_READ_RESULTS),
        query,
        timeout: Duration::from_secs(timeout_seconds.unwrap_or(DEFAULT_CLIENT_TIMEOUT_SECONDS)),
    })
}

fn parse_client_get_arguments<I>(mut arguments: I) -> Result<Command, CliError>
where
    I: Iterator<Item = OsString>,
{
    let mut destination = None;
    let mut document_id = None;
    let mut timeout_seconds = None;
    while let Some(flag) = arguments.next() {
        let value = arguments.next().ok_or(CliError::Usage)?;
        match flag.to_str() {
            Some("--destination") if destination.is_none() => destination = Some(value),
            Some("--document-id") if document_id.is_none() => {
                document_id = Some(
                    value
                        .to_str()
                        .and_then(|value| value.parse().ok())
                        .ok_or(CliError::Usage)?,
                );
            }
            Some("--timeout-seconds") if timeout_seconds.is_none() => {
                timeout_seconds = Some(parse_bounded_u64(&value, MAXIMUM_CLIENT_TIMEOUT_SECONDS)?);
            }
            _ => return Err(CliError::Usage),
        }
    }
    Ok(Command::ClientGet {
        destination: destination.ok_or(CliError::Usage)?,
        request: GetRequest::new(document_id.ok_or(CliError::Usage)?),
        timeout: Duration::from_secs(timeout_seconds.unwrap_or(DEFAULT_CLIENT_TIMEOUT_SECONDS)),
    })
}

fn parse_bounded_u64(value: &OsString, maximum: u64) -> Result<u64, CliError> {
    value
        .to_str()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| (1..=maximum).contains(value))
        .ok_or(CliError::Usage)
}

fn parse_bounded_usize(value: &OsString, maximum: usize) -> Result<usize, CliError> {
    value
        .to_str()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| (1..=maximum).contains(value))
        .ok_or(CliError::Usage)
}

fn parse_gateway_arguments<I>(mut arguments: I) -> Result<Command, CliError>
where
    I: Iterator<Item = OsString>,
{
    let mut config = None;
    let mut client_id = None;
    while let Some(flag) = arguments.next() {
        let value = arguments.next().ok_or(CliError::Usage)?;
        match flag.to_str() {
            Some("--config") if config.is_none() => config = Some(PathBuf::from(value)),
            Some("--client-id") if client_id.is_none() => client_id = Some(value),
            _ => return Err(CliError::Usage),
        }
    }
    Ok(Command::RunGateway {
        config: config.ok_or(CliError::Usage)?,
        client_id: client_id.ok_or(CliError::Usage)?,
    })
}

fn parse_submit_arguments<I>(mut arguments: I) -> Result<Command, CliError>
where
    I: Iterator<Item = OsString>,
{
    let mut queue_root = None;
    let mut package_root = None;
    while let Some(flag) = arguments.next() {
        let value = arguments.next().ok_or(CliError::Usage)?;
        match flag.to_str() {
            Some("--queue-root") if queue_root.is_none() => queue_root = Some(PathBuf::from(value)),
            Some("--package-root") if package_root.is_none() => {
                package_root = Some(PathBuf::from(value));
            }
            _ => return Err(CliError::Usage),
        }
    }

    Ok(Command::Submit {
        queue_root: queue_root.ok_or(CliError::Usage)?,
        package_root: package_root.ok_or(CliError::Usage)?,
    })
}

fn parse_worker_arguments<I>(mut arguments: I) -> Result<Command, CliError>
where
    I: Iterator<Item = OsString>,
{
    let mut config = None;
    while let Some(flag) = arguments.next() {
        let value = arguments.next().ok_or(CliError::Usage)?;
        match flag.to_str() {
            Some("--config") if config.is_none() => config = Some(PathBuf::from(value)),
            _ => return Err(CliError::Usage),
        }
    }
    Ok(Command::RunWorker {
        config: config.ok_or(CliError::Usage)?,
    })
}

fn submit_directory<W>(
    queue_root: &Path,
    package_root: &Path,
    mut output: W,
) -> Result<(), CliError>
where
    W: Write,
{
    let policy = PackagePolicy::default();
    let validated = validate_package(package_root, &policy).map_err(CliError::PackageValidation)?;
    let queue = FileQueue::initialize(queue_root, policy).map_err(CliError::Queue)?;
    let mut incoming = queue.begin().map_err(CliError::Queue)?;

    let mut request = File::open(package_root.join("request.json")).map_err(CliError::Io)?;
    incoming
        .write_request(&mut request)
        .map_err(CliError::Queue)?;
    for payload in validated.payload() {
        let mut source = File::open(package_root.join("payload").join(payload.path().as_str()))
            .map_err(CliError::Io)?;
        incoming
            .add_payload(payload.path().clone(), &mut source)
            .map_err(CliError::Queue)?;
    }

    let response = SubmitResponse::from(incoming.accept().map_err(CliError::Queue)?);
    serde_json::to_writer(&mut output, &response).map_err(CliError::Json)?;
    output.write_all(b"\n").map_err(CliError::Io)
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum SubmitResponse {
    Accepted {
        request_id: String,
        digest: String,
    },
    Existing {
        request_id: String,
        digest: String,
        state: String,
    },
}

impl From<EnqueueOutcome> for SubmitResponse {
    fn from(outcome: EnqueueOutcome) -> Self {
        match outcome {
            EnqueueOutcome::Accepted { request_id, digest } => Self::Accepted {
                request_id: request_id.to_string(),
                digest: digest.to_string(),
            },
            EnqueueOutcome::Existing {
                request_id,
                digest,
                state,
            } => Self::Existing {
                request_id: request_id.to_string(),
                digest: digest.to_string(),
                state: state.to_string(),
            },
        }
    }
}

#[derive(Debug)]
pub enum CliError {
    Usage,
    Io(io::Error),
    PackageValidation(PackageValidationError),
    Queue(QueueError),
    Worker(WorkerCommandError),
    Gateway(GatewayCommandError),
    Client(ClientCommandError),
    Json(serde_json::Error),
}

impl CliError {
    pub fn write_diagnostic(&self, mut output: impl Write) -> io::Result<()> {
        match self {
            Self::Gateway(error) => error.write_protocol_error(output),
            Self::Client(error) => error.write_diagnostic(output),
            _ => writeln!(output, "{self}"),
        }
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage => formatter.write_str(USAGE),
            Self::Io(error) => write!(formatter, "local submission I/O failed: {error}"),
            Self::PackageValidation(error) => {
                write!(formatter, "local package validation failed: {error}")
            }
            Self::Queue(error) => write!(formatter, "durable queue submission failed: {error}"),
            Self::Worker(error) => error.fmt(formatter),
            Self::Gateway(error) => error.fmt(formatter),
            Self::Client(error) => error.fmt(formatter),
            Self::Json(error) => write!(formatter, "JSON output encoding failed: {error}"),
        }
    }
}

impl std::error::Error for CliError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::PackageValidation(error) => Some(error),
            Self::Queue(error) => Some(error),
            Self::Worker(error) => Some(error),
            Self::Gateway(error) => Some(error),
            Self::Client(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::Usage => None,
        }
    }
}

#[cfg(test)]
mod tests;

use std::ffi::OsString;
use std::fmt;
use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use agent_knowledge_queue::{
    EnqueueOutcome, FileQueue, PackagePolicy, PackageValidationError, QueueError, validate_package,
};
use serde::Serialize;

const USAGE: &str = "usage: agent-knowledge admin submit \
    --queue-root <path> --package-root <path>";

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
    }
}

enum Command {
    Submit {
        queue_root: PathBuf,
        package_root: PathBuf,
    },
}

fn parse_arguments<I>(arguments: I) -> Result<Command, CliError>
where
    I: IntoIterator<Item = OsString>,
{
    let mut arguments = arguments.into_iter();
    let _program = arguments.next();
    if arguments.next().as_deref() != Some(std::ffi::OsStr::new("admin"))
        || arguments.next().as_deref() != Some(std::ffi::OsStr::new("submit"))
    {
        return Err(CliError::Usage);
    }

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
    serde_json::to_writer(&mut output, &response).map_err(CliError::Response)?;
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
    Response(serde_json::Error),
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
            Self::Response(error) => write!(formatter, "response JSON encoding failed: {error}"),
        }
    }
}

impl std::error::Error for CliError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::PackageValidation(error) => Some(error),
            Self::Queue(error) => Some(error),
            Self::Response(error) => Some(error),
            Self::Usage => None,
        }
    }
}

#[cfg(test)]
mod tests;

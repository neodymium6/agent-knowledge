use std::ffi::OsString;
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Write};
use std::num::NonZeroUsize;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use agent_knowledge_gateway::IngressServeError;
use agent_knowledge_protocol::{
    ExportRequest, GetRequest, LIST_COMMAND, ListRequest, ListResponse, RECENT_COMMAND,
    ReadFilterRequest, SEARCH_COMMAND, SearchRequest, StatusRequest,
};
use agent_knowledge_queue::{
    EnqueueOutcome, FileQueue, PackagePolicy, PackageValidationError, QueueError, validate_package,
};
#[cfg(unix)]
use nix::unistd::Uid;
use serde::Serialize;

use crate::admin::{self, AdminRetentionError, AdminStatusError};
#[cfg(target_os = "linux")]
use crate::admin::{StorageMigration, StorageMigrationError};
use crate::client::{self, ClientCommandError};
use crate::gateway::{self, GatewayCommandError};
use crate::queue_ingress::{self, ListenSettings, QueueIngressCommandError};
#[cfg(target_os = "linux")]
use crate::storage_bootstrap::{StorageBootstrap, StorageBootstrapError};
use crate::worker::{self, WorkerCommandError};

const COMMON_USAGE: &str = "usage:\n\
    agent-knowledge admin submit --queue-root <path> --package-root <path>\n\
    agent-knowledge admin status --config <path> [--maximum-queue-entries <count>] [--timeout-seconds <seconds>]\n\
    agent-knowledge admin prune-releases --config <path> [--dry-run] [--timeout-seconds <seconds>]\n\
    agent-knowledge client submit --destination <ssh-destination> --package-root <path> [--timeout-seconds <seconds>]\n\
    agent-knowledge client list --destination <ssh-destination> [--project <id>] [--tag <tag>] [--session <id>] [--include-archived] [--maximum-results <count>] [--timeout-seconds <seconds>]\n\
    agent-knowledge client recent --destination <ssh-destination> [--project <id>] [--tag <tag>] [--session <id>] [--include-archived] [--maximum-results <count>] [--timeout-seconds <seconds>]\n\
    agent-knowledge client get --destination <ssh-destination> --document-id <id> [--timeout-seconds <seconds>]\n\
    agent-knowledge client export --destination <ssh-destination> --document-id <id> [--timeout-seconds <seconds>]\n\
    agent-knowledge client status --destination <ssh-destination> --request-id <id> [--timeout-seconds <seconds>]\n\
    agent-knowledge client search --destination <ssh-destination> --query <text> [--project <id>] [--tag <tag>] [--session <id>] [--include-archived] [--maximum-results <count>] [--timeout-seconds <seconds>]\n\
    agent-knowledge gateway --config <path> --client-id <id>\n\
    agent-knowledge queue-ingress serve --queue-root <path>\n\
    agent-knowledge queue-ingress listen --queue-root <path> --socket-path <path> [--maximum-connections <count>] [--connection-timeout-seconds <seconds>]\n\
    agent-knowledge worker run --config <path>";
#[cfg(target_os = "linux")]
const LINUX_USAGE: &str = "\n\
    agent-knowledge admin bootstrap-storage --config <path> [--runtime-directory <path>] [--worker-owner <name-or-id>] [--worker-group <name-or-id>] [--queue-owner <name-or-id>] [--queue-group <name-or-id>] [--gateway-group <name-or-id>] [--ingress-group <name-or-id>]\n\
    agent-knowledge admin migrate-v1-storage --queue-root <path> --git-directory <path> --content-root <path> [--queue-owner <name-or-id>] [--queue-group <name-or-id>] [--gateway-group <name-or-id>]";
const DEFAULT_CLIENT_TIMEOUT_SECONDS: u64 = 300;
const MAXIMUM_CLIENT_TIMEOUT_SECONDS: u64 = 3_600;
const DEFAULT_READ_RESULTS: usize = 100;
const MAXIMUM_READ_RESULTS: usize = 10_000;
const DEFAULT_STATUS_QUEUE_ENTRIES: usize = 100_000;
const MAXIMUM_STATUS_QUEUE_ENTRIES: usize = 1_000_000;
const DEFAULT_STATUS_TIMEOUT_SECONDS: u64 = 30;
const MAXIMUM_STATUS_TIMEOUT_SECONDS: u64 = 300;
const DEFAULT_RETENTION_TIMEOUT_SECONDS: u64 = 300;
const MAXIMUM_RETENTION_TIMEOUT_SECONDS: u64 = 3_600;
const DEFAULT_INGRESS_MAXIMUM_CONNECTIONS: usize = 64;
const MAXIMUM_INGRESS_CONNECTIONS: usize = 1_024;
const DEFAULT_INGRESS_CONNECTION_TIMEOUT_SECONDS: u64 = 3_900;
const MAXIMUM_INGRESS_CONNECTION_TIMEOUT_SECONDS: u64 = 3_900;

pub fn run<I, W>(arguments: I, output: W) -> Result<(), CliError>
where
    I: IntoIterator<Item = OsString>,
    W: Write + Send + 'static,
{
    match parse_arguments(arguments)? {
        Command::Submit {
            queue_root,
            package_root,
        } => submit_directory(&queue_root, &package_root, output),
        Command::AdminStatus {
            config,
            maximum_queue_entries,
            timeout,
        } => admin::status(&config, maximum_queue_entries, timeout, output)
            .map_err(CliError::AdminStatus),
        Command::AdminPruneReleases {
            config,
            dry_run,
            timeout,
        } => admin::prune_releases(&config, dry_run, timeout, output)
            .map_err(CliError::AdminRetention),
        #[cfg(target_os = "linux")]
        Command::AdminBootstrapStorage(settings) => {
            crate::storage_bootstrap::bootstrap_storage(&settings, output)
                .map_err(CliError::StorageBootstrap)
        }
        #[cfg(target_os = "linux")]
        Command::AdminMigrateV1Storage {
            queue_root,
            git_directory,
            content_root,
            queue_owner,
            queue_group,
            gateway_group,
        } => admin::migrate_v1_storage_permissions(
            StorageMigration {
                queue_root: &queue_root,
                git_directory: &git_directory,
                content_root: &content_root,
                queue_owner: &queue_owner,
                queue_group: &queue_group,
                gateway_group: &gateway_group,
            },
            output,
        )
        .map_err(CliError::StorageMigration),
        Command::RunWorker { config } => worker::run(&config, output).map_err(CliError::Worker),
        Command::RunGateway { config, client_id } => gateway::run_stdio(
            &config,
            &client_id,
            std::env::var_os("SSH_ORIGINAL_COMMAND"),
        )
        .map_err(CliError::Gateway),
        Command::ServeQueueIngress { queue_root } => {
            queue_ingress::enforce_writer_umask();
            agent_knowledge_gateway::serve_ingress(&queue_root, io::stdin().lock(), output)
                .map_err(CliError::IngressServe)
        }
        Command::ListenQueueIngress { settings } => {
            queue_ingress::run(settings, output).map_err(CliError::IngressListen)
        }
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
        Command::ClientExport {
            destination,
            request,
            timeout,
        } => client::export(&destination, &request, timeout, output).map_err(CliError::Client),
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
        Command::ClientStatus {
            destination,
            request,
            timeout,
        } => client::status(&destination, &request, timeout, output).map_err(CliError::Client),
    }
}

enum Command {
    Submit {
        queue_root: PathBuf,
        package_root: PathBuf,
    },
    AdminStatus {
        config: PathBuf,
        maximum_queue_entries: usize,
        timeout: Duration,
    },
    AdminPruneReleases {
        config: PathBuf,
        dry_run: bool,
        timeout: Duration,
    },
    #[cfg(target_os = "linux")]
    AdminBootstrapStorage(StorageBootstrap),
    #[cfg(target_os = "linux")]
    AdminMigrateV1Storage {
        queue_root: PathBuf,
        git_directory: PathBuf,
        content_root: PathBuf,
        queue_owner: OsString,
        queue_group: OsString,
        gateway_group: OsString,
    },
    RunWorker {
        config: PathBuf,
    },
    RunGateway {
        config: PathBuf,
        client_id: OsString,
    },
    ServeQueueIngress {
        queue_root: PathBuf,
    },
    ListenQueueIngress {
        settings: ListenSettings,
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
    ClientExport {
        destination: OsString,
        request: ExportRequest,
        timeout: Duration,
    },
    ClientSearch {
        destination: OsString,
        request: SearchRequest,
        timeout: Duration,
    },
    ClientStatus {
        destination: OsString,
        request: StatusRequest,
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
        (Some(namespace), Some(action)) if namespace == std::ffi::OsStr::new("queue-ingress") => {
            parse_queue_ingress_arguments(arguments, action)
        }
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
            parse_client_document_arguments(arguments, false)
        }
        (Some(namespace), Some(action))
            if namespace == std::ffi::OsStr::new("client")
                && action == std::ffi::OsStr::new("export") =>
        {
            parse_client_document_arguments(arguments, true)
        }
        (Some(namespace), Some(action))
            if namespace == std::ffi::OsStr::new("client")
                && action == std::ffi::OsStr::new("search") =>
        {
            parse_client_search_arguments(arguments)
        }
        (Some(namespace), Some(action))
            if namespace == std::ffi::OsStr::new("client")
                && action == std::ffi::OsStr::new("status") =>
        {
            parse_client_status_arguments(arguments)
        }
        (Some(namespace), Some(action))
            if namespace == std::ffi::OsStr::new("admin")
                && action == std::ffi::OsStr::new("submit") =>
        {
            parse_submit_arguments(arguments)
        }
        (Some(namespace), Some(action))
            if namespace == std::ffi::OsStr::new("admin")
                && action == std::ffi::OsStr::new("status") =>
        {
            parse_admin_status_arguments(arguments)
        }
        (Some(namespace), Some(action))
            if namespace == std::ffi::OsStr::new("admin")
                && action == std::ffi::OsStr::new("prune-releases") =>
        {
            parse_admin_prune_releases_arguments(arguments)
        }
        #[cfg(target_os = "linux")]
        (Some(namespace), Some(action))
            if namespace == std::ffi::OsStr::new("admin")
                && action == std::ffi::OsStr::new("bootstrap-storage") =>
        {
            parse_admin_bootstrap_storage_arguments(arguments)
        }
        #[cfg(target_os = "linux")]
        (Some(namespace), Some(action))
            if namespace == std::ffi::OsStr::new("admin")
                && action == std::ffi::OsStr::new("migrate-v1-storage") =>
        {
            parse_admin_migrate_v1_storage_arguments(arguments)
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

fn parse_client_document_arguments<I>(mut arguments: I, export: bool) -> Result<Command, CliError>
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
    let destination = destination.ok_or(CliError::Usage)?;
    let document_id = document_id.ok_or(CliError::Usage)?;
    let timeout = Duration::from_secs(timeout_seconds.unwrap_or(DEFAULT_CLIENT_TIMEOUT_SECONDS));
    if export {
        Ok(Command::ClientExport {
            destination,
            request: ExportRequest::new(document_id),
            timeout,
        })
    } else {
        Ok(Command::ClientGet {
            destination,
            request: GetRequest::new(document_id),
            timeout,
        })
    }
}

fn parse_client_status_arguments<I>(mut arguments: I) -> Result<Command, CliError>
where
    I: Iterator<Item = OsString>,
{
    let mut destination = None;
    let mut request_id = None;
    let mut timeout_seconds = None;
    while let Some(flag) = arguments.next() {
        let value = arguments.next().ok_or(CliError::Usage)?;
        match flag.to_str() {
            Some("--destination") if destination.is_none() => destination = Some(value),
            Some("--request-id") if request_id.is_none() => {
                request_id = Some(
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
    Ok(Command::ClientStatus {
        destination: destination.ok_or(CliError::Usage)?,
        request: StatusRequest::new(request_id.ok_or(CliError::Usage)?),
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

fn parse_queue_ingress_arguments<I>(
    mut arguments: I,
    action: &std::ffi::OsStr,
) -> Result<Command, CliError>
where
    I: Iterator<Item = OsString>,
{
    let mut queue_root = None;
    let mut socket_path = None;
    let mut maximum_connections = None;
    let mut connection_timeout_seconds = None;
    while let Some(flag) = arguments.next() {
        let value = arguments.next().ok_or(CliError::Usage)?;
        match flag.to_str() {
            Some("--queue-root") if queue_root.is_none() => {
                queue_root = Some(PathBuf::from(value));
            }
            Some("--socket-path") if socket_path.is_none() => {
                socket_path = Some(PathBuf::from(value));
            }
            Some("--maximum-connections") if maximum_connections.is_none() => {
                maximum_connections =
                    Some(parse_bounded_usize(&value, MAXIMUM_INGRESS_CONNECTIONS)?);
            }
            Some("--connection-timeout-seconds") if connection_timeout_seconds.is_none() => {
                connection_timeout_seconds = Some(parse_bounded_u64(
                    &value,
                    MAXIMUM_INGRESS_CONNECTION_TIMEOUT_SECONDS,
                )?);
            }
            _ => return Err(CliError::Usage),
        }
    }
    let queue_root = queue_root.ok_or(CliError::Usage)?;
    if action == std::ffi::OsStr::new("serve")
        && socket_path.is_none()
        && maximum_connections.is_none()
        && connection_timeout_seconds.is_none()
    {
        return Ok(Command::ServeQueueIngress { queue_root });
    }
    if action != std::ffi::OsStr::new("listen") {
        return Err(CliError::Usage);
    }
    Ok(Command::ListenQueueIngress {
        settings: ListenSettings {
            queue_root,
            socket_path: socket_path.ok_or(CliError::Usage)?,
            maximum_connections: NonZeroUsize::new(
                maximum_connections.unwrap_or(DEFAULT_INGRESS_MAXIMUM_CONNECTIONS),
            )
            .ok_or(CliError::Usage)?,
            connection_timeout: Duration::from_secs(
                connection_timeout_seconds.unwrap_or(DEFAULT_INGRESS_CONNECTION_TIMEOUT_SECONDS),
            ),
            #[cfg(test)]
            deadline_observer: None,
            #[cfg(test)]
            handler_blocker: None,
        },
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

fn parse_admin_status_arguments<I>(mut arguments: I) -> Result<Command, CliError>
where
    I: Iterator<Item = OsString>,
{
    let mut config = None;
    let mut maximum_queue_entries = None;
    let mut timeout_seconds = None;
    while let Some(flag) = arguments.next() {
        let value = arguments.next().ok_or(CliError::Usage)?;
        match flag.to_str() {
            Some("--config") if config.is_none() => config = Some(PathBuf::from(value)),
            Some("--maximum-queue-entries") if maximum_queue_entries.is_none() => {
                maximum_queue_entries =
                    Some(parse_bounded_usize(&value, MAXIMUM_STATUS_QUEUE_ENTRIES)?);
            }
            Some("--timeout-seconds") if timeout_seconds.is_none() => {
                timeout_seconds = Some(parse_bounded_u64(&value, MAXIMUM_STATUS_TIMEOUT_SECONDS)?);
            }
            _ => return Err(CliError::Usage),
        }
    }
    Ok(Command::AdminStatus {
        config: config.ok_or(CliError::Usage)?,
        maximum_queue_entries: maximum_queue_entries.unwrap_or(DEFAULT_STATUS_QUEUE_ENTRIES),
        timeout: Duration::from_secs(timeout_seconds.unwrap_or(DEFAULT_STATUS_TIMEOUT_SECONDS)),
    })
}

fn parse_admin_prune_releases_arguments<I>(mut arguments: I) -> Result<Command, CliError>
where
    I: Iterator<Item = OsString>,
{
    let mut config = None;
    let mut dry_run = false;
    let mut timeout_seconds = None;
    while let Some(flag) = arguments.next() {
        if flag == std::ffi::OsStr::new("--dry-run") {
            if dry_run {
                return Err(CliError::Usage);
            }
            dry_run = true;
            continue;
        }
        let value = arguments.next().ok_or(CliError::Usage)?;
        match flag.to_str() {
            Some("--config") if config.is_none() => config = Some(PathBuf::from(value)),
            Some("--timeout-seconds") if timeout_seconds.is_none() => {
                timeout_seconds = Some(parse_bounded_u64(
                    &value,
                    MAXIMUM_RETENTION_TIMEOUT_SECONDS,
                )?);
            }
            _ => return Err(CliError::Usage),
        }
    }
    Ok(Command::AdminPruneReleases {
        config: config.ok_or(CliError::Usage)?,
        dry_run,
        timeout: Duration::from_secs(timeout_seconds.unwrap_or(DEFAULT_RETENTION_TIMEOUT_SECONDS)),
    })
}

#[cfg(target_os = "linux")]
fn parse_admin_migrate_v1_storage_arguments<I>(mut arguments: I) -> Result<Command, CliError>
where
    I: Iterator<Item = OsString>,
{
    let mut queue_root = None;
    let mut git_directory = None;
    let mut content_root = None;
    let mut queue_owner = None;
    let mut queue_group = None;
    let mut gateway_group = None;
    while let Some(flag) = arguments.next() {
        let value = arguments.next().ok_or(CliError::Usage)?;
        match flag.to_str() {
            Some("--queue-root") if queue_root.is_none() => {
                queue_root = Some(PathBuf::from(value));
            }
            Some("--git-directory") if git_directory.is_none() => {
                git_directory = Some(PathBuf::from(value));
            }
            Some("--content-root") if content_root.is_none() => {
                content_root = Some(PathBuf::from(value));
            }
            Some("--queue-owner") if queue_owner.is_none() => queue_owner = Some(value),
            Some("--queue-group") if queue_group.is_none() => queue_group = Some(value),
            Some("--gateway-group") if gateway_group.is_none() => gateway_group = Some(value),
            _ => return Err(CliError::Usage),
        }
    }
    Ok(Command::AdminMigrateV1Storage {
        queue_root: queue_root.ok_or(CliError::Usage)?,
        git_directory: git_directory.ok_or(CliError::Usage)?,
        content_root: content_root.ok_or(CliError::Usage)?,
        queue_owner: queue_owner.unwrap_or_else(|| "agent-knowledge-queue".into()),
        queue_group: queue_group.unwrap_or_else(|| "agent-knowledge-queue".into()),
        gateway_group: gateway_group.unwrap_or_else(|| "agent-knowledge-gateway".into()),
    })
}

#[cfg(target_os = "linux")]
fn parse_admin_bootstrap_storage_arguments<I>(mut arguments: I) -> Result<Command, CliError>
where
    I: Iterator<Item = OsString>,
{
    let mut config = None;
    let mut runtime_directory = None;
    let mut worker_owner = None;
    let mut worker_group = None;
    let mut queue_owner = None;
    let mut queue_group = None;
    let mut gateway_group = None;
    let mut ingress_group = None;
    while let Some(flag) = arguments.next() {
        let value = arguments.next().ok_or(CliError::Usage)?;
        match flag.to_str() {
            Some("--config") if config.is_none() => config = Some(PathBuf::from(value)),
            Some("--runtime-directory") if runtime_directory.is_none() => {
                runtime_directory = Some(PathBuf::from(value));
            }
            Some("--worker-owner") if worker_owner.is_none() => worker_owner = Some(value),
            Some("--worker-group") if worker_group.is_none() => worker_group = Some(value),
            Some("--queue-owner") if queue_owner.is_none() => queue_owner = Some(value),
            Some("--queue-group") if queue_group.is_none() => queue_group = Some(value),
            Some("--gateway-group") if gateway_group.is_none() => gateway_group = Some(value),
            Some("--ingress-group") if ingress_group.is_none() => ingress_group = Some(value),
            _ => return Err(CliError::Usage),
        }
    }
    Ok(Command::AdminBootstrapStorage(StorageBootstrap {
        config: config.ok_or(CliError::Usage)?,
        runtime_directory: runtime_directory
            .unwrap_or_else(|| PathBuf::from("/run/agent-knowledge")),
        worker_owner: worker_owner.unwrap_or_else(|| "agent-knowledge".into()),
        worker_group: worker_group.unwrap_or_else(|| "agent-knowledge".into()),
        queue_owner: queue_owner.unwrap_or_else(|| "agent-knowledge-queue".into()),
        queue_group: queue_group.unwrap_or_else(|| "agent-knowledge-queue".into()),
        gateway_group: gateway_group.unwrap_or_else(|| "agent-knowledge-gateway".into()),
        ingress_group: ingress_group.unwrap_or_else(|| "agent-knowledge-ingress".into()),
    }))
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
    queue_ingress::enforce_writer_umask();
    require_local_queue_owner(queue_root)?;
    let policy = PackagePolicy::default();
    let validated = validate_package(package_root, &policy).map_err(CliError::PackageValidation)?;
    let queue = FileQueue::initialize(queue_root, policy).map_err(CliError::Queue)?;
    require_local_queue_owner(queue_root)?;
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

#[cfg(unix)]
fn require_local_queue_owner(queue_root: &Path) -> Result<(), CliError> {
    match fs::symlink_metadata(queue_root) {
        Ok(metadata)
            if metadata.file_type().is_dir() && metadata.uid() == Uid::effective().as_raw() =>
        {
            Ok(())
        }
        Ok(_) => Err(CliError::LocalQueueOwner(queue_root.to_path_buf())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(CliError::Io(error)),
    }
}

#[cfg(not(unix))]
fn require_local_queue_owner(_queue_root: &Path) -> Result<(), CliError> {
    Ok(())
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
    LocalQueueOwner(PathBuf),
    AdminStatus(AdminStatusError),
    AdminRetention(AdminRetentionError),
    #[cfg(target_os = "linux")]
    StorageMigration(StorageMigrationError),
    #[cfg(target_os = "linux")]
    StorageBootstrap(StorageBootstrapError),
    Worker(WorkerCommandError),
    Gateway(GatewayCommandError),
    IngressServe(IngressServeError),
    IngressListen(QueueIngressCommandError),
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
            Self::Usage => {
                formatter.write_str(COMMON_USAGE)?;
                #[cfg(target_os = "linux")]
                formatter.write_str(LINUX_USAGE)?;
                Ok(())
            }
            Self::Io(error) => write!(formatter, "local submission I/O failed: {error}"),
            Self::PackageValidation(error) => {
                write!(formatter, "local package validation failed: {error}")
            }
            Self::Queue(error) => write!(formatter, "durable queue submission failed: {error}"),
            Self::LocalQueueOwner(path) => write!(
                formatter,
                "local queue submission must run as the owner of {}",
                path.display()
            ),
            Self::AdminStatus(error) => error.fmt(formatter),
            Self::AdminRetention(error) => error.fmt(formatter),
            #[cfg(target_os = "linux")]
            Self::StorageMigration(error) => error.fmt(formatter),
            #[cfg(target_os = "linux")]
            Self::StorageBootstrap(error) => error.fmt(formatter),
            Self::Worker(error) => error.fmt(formatter),
            Self::Gateway(error) => error.fmt(formatter),
            Self::IngressServe(error) => error.fmt(formatter),
            Self::IngressListen(error) => error.fmt(formatter),
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
            Self::LocalQueueOwner(_) => None,
            Self::AdminStatus(error) => Some(error),
            Self::AdminRetention(error) => Some(error),
            #[cfg(target_os = "linux")]
            Self::StorageMigration(error) => Some(error),
            #[cfg(target_os = "linux")]
            Self::StorageBootstrap(error) => Some(error),
            Self::Worker(error) => Some(error),
            Self::Gateway(error) => Some(error),
            Self::IngressServe(error) => Some(error),
            Self::IngressListen(error) => Some(error),
            Self::Client(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::Usage => None,
        }
    }
}

#[cfg(test)]
mod tests;

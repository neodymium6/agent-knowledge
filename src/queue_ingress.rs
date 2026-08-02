use std::collections::HashMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::num::NonZeroUsize;
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use agent_knowledge_core::{PathAttestation, PathAttestationError};
use agent_knowledge_gateway::IngressServeError;
use nix::errno::Errno;
use nix::fcntl::{Flock, FlockArg};
use signal_hook::consts::{SIGINT, SIGTERM};

const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(50);
const LISTENER_LOCK_FILE: &str = ".agent-knowledge-queue-ingress.lock";
const SOCKET_MODE: u32 = 0o660;

#[derive(Clone, Debug)]
pub(crate) struct ListenSettings {
    pub(crate) queue_root: PathBuf,
    pub(crate) socket_path: PathBuf,
    pub(crate) maximum_connections: NonZeroUsize,
    pub(crate) connection_timeout: Duration,
}

pub(crate) fn run<W>(settings: ListenSettings, output: W) -> Result<(), QueueIngressCommandError>
where
    W: Write,
{
    let stopping = Arc::new(AtomicBool::new(false));
    let _sigint = signal_hook::flag::register(SIGINT, Arc::clone(&stopping))
        .map_err(QueueIngressCommandError::SignalRegistration)?;
    let _sigterm = signal_hook::flag::register(SIGTERM, Arc::clone(&stopping))
        .map_err(QueueIngressCommandError::SignalRegistration)?;
    listen_until(settings, output, || stopping.load(Ordering::Relaxed))
        .map_err(QueueIngressCommandError::Listener)
}

fn listen_until<W>(
    settings: ListenSettings,
    mut output: W,
    should_stop: impl Fn() -> bool,
) -> Result<(), QueueIngressListenerError>
where
    W: Write,
{
    let published = PublishedSocket::bind(&settings.socket_path)?;
    let (completion_sender, completion_receiver) = mpsc::channel();
    let mut active = HashMap::<u64, ActiveConnection>::new();
    let mut next_connection = 0_u64;
    let mut terminal_error = None;

    while !should_stop() {
        if let Err(error) = drain_completions(&completion_receiver, &mut active, &mut output) {
            terminal_error = Some(error);
            break;
        }
        expire_connections(&active, Instant::now());
        if active.len() >= settings.maximum_connections.get() {
            thread::sleep(ACCEPT_POLL_INTERVAL);
            continue;
        }

        match published.listener.accept() {
            Ok((stream, _address)) => {
                let connection_id = next_connection;
                next_connection = next_connection.wrapping_add(1);
                match start_connection(connection_id, stream, &settings, &completion_sender) {
                    Ok(connection) => {
                        active.insert(connection_id, connection);
                    }
                    Err(error) => {
                        terminal_error = Some(error);
                        break;
                    }
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(ACCEPT_POLL_INTERVAL);
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => {
                terminal_error = Some(QueueIngressListenerError::Accept(error));
                break;
            }
        }
    }

    for connection in active.values() {
        let _ = connection.control.shutdown(std::net::Shutdown::Both);
    }
    drop(completion_sender);
    while !active.is_empty() {
        match completion_receiver.recv() {
            Ok(completion) => {
                if let Err(error) = finish_connection(completion, &mut active, &mut output, false)
                    && terminal_error.is_none()
                {
                    terminal_error = Some(error);
                }
            }
            Err(_) => {
                if terminal_error.is_none() {
                    terminal_error = Some(QueueIngressListenerError::CompletionChannelClosed);
                }
                break;
            }
        }
    }

    match terminal_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn start_connection(
    connection_id: u64,
    stream: UnixStream,
    settings: &ListenSettings,
    completion_sender: &Sender<ConnectionCompletion>,
) -> Result<ActiveConnection, QueueIngressListenerError> {
    stream
        .set_read_timeout(Some(settings.connection_timeout))
        .map_err(QueueIngressListenerError::ConnectionConfiguration)?;
    stream
        .set_write_timeout(Some(settings.connection_timeout))
        .map_err(QueueIngressListenerError::ConnectionConfiguration)?;
    let control = stream
        .try_clone()
        .map_err(QueueIngressListenerError::ConnectionConfiguration)?;
    let deadline = Instant::now()
        .checked_add(settings.connection_timeout)
        .ok_or(QueueIngressListenerError::InvalidConnectionTimeout)?;
    let queue_root = settings.queue_root.clone();
    let sender = completion_sender.clone();
    let thread = thread::Builder::new()
        .name(format!("queue-ingress-{connection_id}"))
        .spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                agent_knowledge_gateway::serve_ingress(&queue_root, &stream, &stream)
            }));
            let outcome = match result {
                Ok(result) => ConnectionOutcome::Served(result),
                Err(_) => ConnectionOutcome::Panicked,
            };
            let _ = sender.send(ConnectionCompletion {
                connection_id,
                outcome,
            });
        })
        .map_err(QueueIngressListenerError::ThreadSpawn)?;
    Ok(ActiveConnection {
        control,
        deadline,
        thread,
    })
}

fn expire_connections(active: &HashMap<u64, ActiveConnection>, now: Instant) {
    for connection in active.values() {
        if now >= connection.deadline {
            let _ = connection.control.shutdown(std::net::Shutdown::Both);
        }
    }
}

fn drain_completions<W>(
    receiver: &Receiver<ConnectionCompletion>,
    active: &mut HashMap<u64, ActiveConnection>,
    output: &mut W,
) -> Result<(), QueueIngressListenerError>
where
    W: Write,
{
    loop {
        match receiver.try_recv() {
            Ok(completion) => finish_connection(completion, active, output, true)?,
            Err(TryRecvError::Empty) => return Ok(()),
            Err(TryRecvError::Disconnected) if active.is_empty() => return Ok(()),
            Err(TryRecvError::Disconnected) => {
                return Err(QueueIngressListenerError::CompletionChannelClosed);
            }
        }
    }
}

fn finish_connection<W>(
    completion: ConnectionCompletion,
    active: &mut HashMap<u64, ActiveConnection>,
    output: &mut W,
    report_failure: bool,
) -> Result<(), QueueIngressListenerError>
where
    W: Write,
{
    let connection = active
        .remove(&completion.connection_id)
        .ok_or(QueueIngressListenerError::UnknownConnection)?;
    connection
        .thread
        .join()
        .map_err(|_| QueueIngressListenerError::ConnectionPanicked)?;
    match completion.outcome {
        ConnectionOutcome::Served(Ok(())) => Ok(()),
        ConnectionOutcome::Served(Err(error)) if report_failure => {
            writeln!(output, "queue ingress connection failed: {error}")
                .map_err(QueueIngressListenerError::Diagnostic)
        }
        ConnectionOutcome::Served(Err(_)) => Ok(()),
        ConnectionOutcome::Panicked => Err(QueueIngressListenerError::ConnectionPanicked),
    }
}

struct ActiveConnection {
    control: UnixStream,
    deadline: Instant,
    thread: JoinHandle<()>,
}

struct ConnectionCompletion {
    connection_id: u64,
    outcome: ConnectionOutcome,
}

enum ConnectionOutcome {
    Served(Result<(), IngressServeError>),
    Panicked,
}

#[derive(Debug)]
struct PublishedSocket {
    listener: UnixListener,
    _socket_node: OwnedSocketNode,
    _parent: PathAttestation,
    _lock: Flock<File>,
}

impl PublishedSocket {
    fn bind(socket_path: &Path) -> Result<Self, QueueIngressListenerError> {
        if !socket_path.is_absolute() || socket_path.file_name().is_none() {
            return Err(QueueIngressListenerError::InvalidSocketPath);
        }
        let parent_path = socket_path
            .parent()
            .ok_or(QueueIngressListenerError::InvalidSocketPath)?;
        let parent = PathAttestation::resolve_destination(parent_path)
            .map_err(QueueIngressListenerError::ParentAttestation)?;
        let parent_metadata = fs::metadata(parent.stable_path())
            .map_err(QueueIngressListenerError::ParentInspection)?;
        if !parent_metadata.is_dir() {
            return Err(QueueIngressListenerError::InvalidSocketParent);
        }

        let lock_path = parent.path().join(LISTENER_LOCK_FILE);
        let lock_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .mode(0o600)
            .custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_NOFOLLOW)
            .open(&lock_path)
            .map_err(QueueIngressListenerError::LockOpen)?;
        let lock_metadata = lock_file
            .metadata()
            .map_err(QueueIngressListenerError::LockInspection)?;
        if !lock_metadata.is_file() || lock_metadata.nlink() != 1 {
            return Err(QueueIngressListenerError::InvalidLockFile);
        }
        lock_file
            .set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(QueueIngressListenerError::LockPermissions)?;
        let lock =
            Flock::lock(lock_file, FlockArg::LockExclusiveNonblock).map_err(|(_file, error)| {
                if error == Errno::EWOULDBLOCK || error == Errno::EAGAIN {
                    QueueIngressListenerError::AlreadyRunning
                } else {
                    QueueIngressListenerError::Lock(io::Error::from_raw_os_error(error as i32))
                }
            })?;

        let stable_socket_path = parent.path().join(
            socket_path
                .file_name()
                .ok_or(QueueIngressListenerError::InvalidSocketPath)?,
        );
        match fs::symlink_metadata(&stable_socket_path) {
            Ok(metadata) if metadata.file_type().is_socket() => {
                match UnixStream::connect(&stable_socket_path) {
                    Ok(_) => return Err(QueueIngressListenerError::AlreadyRunning),
                    Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {
                        fs::remove_file(&stable_socket_path)
                            .map_err(QueueIngressListenerError::StaleSocketRemoval)?;
                    }
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(QueueIngressListenerError::SocketProbe(error)),
                }
            }
            Ok(_) => return Err(QueueIngressListenerError::InvalidSocketTarget),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(QueueIngressListenerError::SocketInspection(error)),
        }

        let listener =
            UnixListener::bind(&stable_socket_path).map_err(QueueIngressListenerError::Bind)?;
        let socket_node = OwnedSocketNode::capture(stable_socket_path.clone())?;
        let observed_parent = PathAttestation::resolve_destination(parent.path())
            .map_err(QueueIngressListenerError::ParentAttestation)?;
        if !parent.matches_destination(&observed_parent) {
            return Err(QueueIngressListenerError::ParentAttestation(
                PathAttestationError::BindingMismatch,
            ));
        }
        fs::set_permissions(&stable_socket_path, fs::Permissions::from_mode(SOCKET_MODE))
            .map_err(QueueIngressListenerError::SocketPermissions)?;
        listener
            .set_nonblocking(true)
            .map_err(QueueIngressListenerError::Nonblocking)?;
        Ok(Self {
            listener,
            _socket_node: socket_node,
            _parent: parent,
            _lock: lock,
        })
    }
}

#[derive(Debug)]
struct OwnedSocketNode {
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl OwnedSocketNode {
    fn capture(path: PathBuf) -> Result<Self, QueueIngressListenerError> {
        let metadata =
            fs::symlink_metadata(&path).map_err(QueueIngressListenerError::SocketInspection)?;
        if !metadata.file_type().is_socket() {
            return Err(QueueIngressListenerError::InvalidSocketTarget);
        }
        Ok(Self {
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
}

impl Drop for OwnedSocketNode {
    fn drop(&mut self) {
        let Ok(metadata) = fs::symlink_metadata(&self.path) else {
            return;
        };
        if metadata.file_type().is_socket()
            && metadata.dev() == self.device
            && metadata.ino() == self.inode
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

#[derive(Debug)]
pub(crate) enum QueueIngressCommandError {
    SignalRegistration(io::Error),
    Listener(QueueIngressListenerError),
}

impl fmt::Display for QueueIngressCommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SignalRegistration(error) => {
                write!(
                    formatter,
                    "could not install queue ingress signal handlers: {error}"
                )
            }
            Self::Listener(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for QueueIngressCommandError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::SignalRegistration(error) => Some(error),
            Self::Listener(error) => Some(error),
        }
    }
}

#[derive(Debug)]
pub(crate) enum QueueIngressListenerError {
    InvalidSocketPath,
    ParentAttestation(PathAttestationError),
    ParentInspection(io::Error),
    InvalidSocketParent,
    LockOpen(io::Error),
    LockInspection(io::Error),
    InvalidLockFile,
    LockPermissions(io::Error),
    AlreadyRunning,
    Lock(io::Error),
    SocketInspection(io::Error),
    SocketProbe(io::Error),
    InvalidSocketTarget,
    StaleSocketRemoval(io::Error),
    Bind(io::Error),
    SocketPermissions(io::Error),
    Nonblocking(io::Error),
    Accept(io::Error),
    ConnectionConfiguration(io::Error),
    InvalidConnectionTimeout,
    ThreadSpawn(io::Error),
    Diagnostic(io::Error),
    CompletionChannelClosed,
    UnknownConnection,
    ConnectionPanicked,
}

impl fmt::Display for QueueIngressListenerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSocketPath => {
                formatter.write_str("queue ingress socket path must be an absolute file path")
            }
            Self::ParentAttestation(error) => {
                write!(
                    formatter,
                    "queue ingress socket parent attestation failed: {error}"
                )
            }
            Self::ParentInspection(error) => {
                write!(
                    formatter,
                    "could not inspect queue ingress socket parent: {error}"
                )
            }
            Self::InvalidSocketParent => {
                formatter.write_str("queue ingress socket parent is not a directory")
            }
            Self::LockOpen(error) => {
                write!(
                    formatter,
                    "could not open queue ingress listener lock: {error}"
                )
            }
            Self::LockInspection(error) => {
                write!(
                    formatter,
                    "could not inspect queue ingress listener lock: {error}"
                )
            }
            Self::InvalidLockFile => {
                formatter.write_str("queue ingress listener lock is not a private regular file")
            }
            Self::LockPermissions(error) => {
                write!(
                    formatter,
                    "could not restrict queue ingress listener lock: {error}"
                )
            }
            Self::AlreadyRunning => {
                formatter.write_str("queue ingress listener is already running")
            }
            Self::Lock(error) => {
                write!(
                    formatter,
                    "could not lock queue ingress listener state: {error}"
                )
            }
            Self::SocketInspection(error) => {
                write!(formatter, "could not inspect queue ingress socket: {error}")
            }
            Self::SocketProbe(error) => {
                write!(
                    formatter,
                    "could not probe existing queue ingress socket: {error}"
                )
            }
            Self::InvalidSocketTarget => formatter
                .write_str("queue ingress socket path exists and is not a Unix domain socket"),
            Self::StaleSocketRemoval(error) => {
                write!(
                    formatter,
                    "could not remove stale queue ingress socket: {error}"
                )
            }
            Self::Bind(error) => write!(formatter, "could not bind queue ingress socket: {error}"),
            Self::SocketPermissions(error) => {
                write!(
                    formatter,
                    "could not set queue ingress socket permissions: {error}"
                )
            }
            Self::Nonblocking(error) => {
                write!(
                    formatter,
                    "could not configure queue ingress listener: {error}"
                )
            }
            Self::Accept(error) => {
                write!(
                    formatter,
                    "could not accept queue ingress connection: {error}"
                )
            }
            Self::ConnectionConfiguration(error) => {
                write!(
                    formatter,
                    "could not configure queue ingress connection: {error}"
                )
            }
            Self::InvalidConnectionTimeout => {
                formatter.write_str("queue ingress connection timeout is outside the clock range")
            }
            Self::ThreadSpawn(error) => {
                write!(
                    formatter,
                    "could not start queue ingress connection thread: {error}"
                )
            }
            Self::Diagnostic(error) => {
                write!(
                    formatter,
                    "could not write queue ingress diagnostic: {error}"
                )
            }
            Self::CompletionChannelClosed => {
                formatter.write_str("queue ingress connection completion channel closed")
            }
            Self::UnknownConnection => {
                formatter.write_str("queue ingress completed an unknown connection")
            }
            Self::ConnectionPanicked => {
                formatter.write_str("queue ingress connection thread panicked")
            }
        }
    }
}

impl std::error::Error for QueueIngressListenerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ParentAttestation(error) => Some(error),
            Self::ParentInspection(error)
            | Self::LockOpen(error)
            | Self::LockInspection(error)
            | Self::LockPermissions(error)
            | Self::Lock(error)
            | Self::SocketInspection(error)
            | Self::SocketProbe(error)
            | Self::StaleSocketRemoval(error)
            | Self::Bind(error)
            | Self::SocketPermissions(error)
            | Self::Nonblocking(error)
            | Self::Accept(error)
            | Self::ConnectionConfiguration(error)
            | Self::ThreadSpawn(error)
            | Self::Diagnostic(error) => Some(error),
            Self::InvalidSocketPath
            | Self::InvalidSocketParent
            | Self::InvalidLockFile
            | Self::AlreadyRunning
            | Self::InvalidSocketTarget
            | Self::InvalidConnectionTimeout
            | Self::CompletionChannelClosed
            | Self::UnknownConnection
            | Self::ConnectionPanicked => None,
        }
    }
}

#[cfg(test)]
#[path = "queue_ingress/tests.rs"]
mod tests;

use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::num::NonZeroUsize;
use std::os::fd::AsRawFd;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender, TryRecvError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use agent_knowledge_core::{PathAttestation, PathAttestationError};
use agent_knowledge_queue::QueueOperationDeadline;
use nix::errno::Errno;
use nix::fcntl::{Flock, FlockArg, RenameFlags, renameat2};
use nix::sys::socket::{AddressFamily, SockFlag, SockType, UnixAddr, connect, socket};
use nix::unistd::Uid;
use signal_hook::consts::{SIGINT, SIGTERM};
use ulid::Ulid;

use crate::runtime_identity::{RuntimeIdentityError, validate_queue_ingress};

const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(50);
const HANDLER_CANCELLATION_GRACE: Duration = Duration::from_secs(1);
const LISTENER_LOCK_FILE: &str = ".agent-knowledge-queue-ingress.lock";
const SOCKET_STATE_FILE: &str = ".agent-knowledge-queue-ingress.socket-state";
const SOCKET_STATE_TEMPORARY_FILE: &str = ".agent-knowledge-queue-ingress.socket-state.tmp";
const SOCKET_STATE_FORMAT: &str = "agent-knowledge-queue-ingress-v2";
const LEGACY_SOCKET_STATE_FORMAT: &str = "agent-knowledge-queue-ingress-v1";
const TEMPORARY_SOCKET_PREFIX: &str = ".ak-";
const MAXIMUM_SOCKET_STATE_BYTES: u64 = 512;
const MAXIMUM_SOCKET_DIRECTORY_ENTRIES: usize = 1_024;
const DIAGNOSTIC_QUEUE_CAPACITY: usize = 1;
const SOCKET_MODE: u32 = 0o660;
#[cfg(not(test))]
const QUEUE_INGRESS_UMASK: u32 = 0o007;

pub(crate) fn enforce_writer_umask() {
    #[cfg(not(test))]
    nix::sys::stat::umask(nix::sys::stat::Mode::from_bits_truncate(
        QUEUE_INGRESS_UMASK,
    ));
}

#[derive(Clone, Debug)]
pub(crate) struct ListenSettings {
    pub(crate) queue_root: PathBuf,
    pub(crate) socket_path: PathBuf,
    pub(crate) maximum_connections: NonZeroUsize,
    pub(crate) connection_timeout: Duration,
    #[cfg(test)]
    pub(crate) deadline_observer: Option<Sender<QueueOperationDeadline>>,
    #[cfg(test)]
    pub(crate) handler_blocker: Option<Arc<(std::sync::Barrier, std::sync::Barrier)>>,
}

pub(crate) fn run<W>(settings: ListenSettings, output: W) -> Result<(), QueueIngressCommandError>
where
    W: Write + Send + 'static,
{
    enforce_writer_umask();
    validate_queue_ingress(&settings.queue_root, &settings.socket_path)
        .map_err(QueueIngressCommandError::Identity)?;
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
    output: W,
    should_stop: impl Fn() -> bool,
) -> Result<(), QueueIngressListenerError>
where
    W: Write + Send + 'static,
{
    let published = PublishedSocket::bind(&settings.socket_path)?;
    let (diagnostic_sender, diagnostic_done) = start_diagnostic_reporter(output)?;
    let report_diagnostics = Arc::new(AtomicBool::new(true));
    let (completion_sender, completion_receiver) = mpsc::channel();
    let mut active = HashMap::<u64, ActiveConnection>::new();
    let mut next_connection = 0_u64;
    let mut terminal_error = None;

    while !should_stop() {
        if let Err(error) = drain_completions(&completion_receiver, &mut active) {
            terminal_error = Some(error);
            break;
        }
        if let Some(connection_id) = expire_connections(&mut active, Instant::now()) {
            terminal_error = Some(QueueIngressListenerError::ConnectionCancellationTimedOut(
                connection_id,
            ));
            break;
        }
        if active.len() >= settings.maximum_connections.get() {
            thread::sleep(ACCEPT_POLL_INTERVAL);
            continue;
        }

        match published.listener.accept() {
            Ok((stream, _address)) => {
                let connection_id = next_connection;
                next_connection = next_connection.wrapping_add(1);
                match start_connection(
                    connection_id,
                    stream,
                    &settings,
                    &completion_sender,
                    &diagnostic_sender,
                    &report_diagnostics,
                ) {
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

    report_diagnostics.store(false, Ordering::Release);
    for connection in active.values() {
        connection.deadline.cancel();
        let _ = connection.control.shutdown(std::net::Shutdown::Both);
    }
    drop(completion_sender);
    let shutdown_deadline = Instant::now()
        .checked_add(HANDLER_CANCELLATION_GRACE)
        .unwrap_or_else(Instant::now);
    while !active.is_empty() && Instant::now() < shutdown_deadline {
        let remaining = shutdown_deadline.saturating_duration_since(Instant::now());
        match completion_receiver.recv_timeout(remaining.min(ACCEPT_POLL_INTERVAL)) {
            Ok(completion) => {
                if let Err(error) = finish_connection(completion, &mut active)
                    && terminal_error.is_none()
                {
                    terminal_error = Some(error);
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                if terminal_error.is_none() {
                    terminal_error = Some(QueueIngressListenerError::CompletionChannelClosed);
                }
                break;
            }
        }
    }

    drop(diagnostic_sender);
    let _ = diagnostic_done.recv_timeout(ACCEPT_POLL_INTERVAL);

    match terminal_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn start_diagnostic_reporter<W>(
    mut output: W,
) -> Result<(SyncSender<String>, Receiver<()>), QueueIngressListenerError>
where
    W: Write + Send + 'static,
{
    let (sender, receiver) = mpsc::sync_channel::<String>(DIAGNOSTIC_QUEUE_CAPACITY);
    let (done_sender, done_receiver) = mpsc::channel();
    thread::Builder::new()
        .name("queue-ingress-diagnostics".into())
        .spawn(move || {
            while let Ok(message) = receiver.recv() {
                if writeln!(output, "{message}").is_err() {
                    break;
                }
            }
            let _ = done_sender.send(());
        })
        .map_err(QueueIngressListenerError::ThreadSpawn)?;
    Ok((sender, done_receiver))
}

fn start_connection(
    connection_id: u64,
    stream: UnixStream,
    settings: &ListenSettings,
    completion_sender: &Sender<ConnectionCompletion>,
    diagnostic_sender: &SyncSender<String>,
    report_diagnostics: &Arc<AtomicBool>,
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
    let expires_at = Instant::now()
        .checked_add(settings.connection_timeout)
        .ok_or(QueueIngressListenerError::InvalidConnectionTimeout)?;
    let deadline = QueueOperationDeadline::new(expires_at);
    #[cfg(test)]
    if let Some(observer) = &settings.deadline_observer {
        let _ = observer.send(deadline.clone());
    }
    let handler_deadline = deadline.clone();
    #[cfg(test)]
    let handler_blocker = settings.handler_blocker.clone();
    let queue_root = settings.queue_root.clone();
    let sender = completion_sender.clone();
    let diagnostic_sender = diagnostic_sender.clone();
    let report_diagnostics = Arc::clone(report_diagnostics);
    let thread = thread::Builder::new()
        .name(format!("queue-ingress-{connection_id}"))
        .spawn(move || {
            #[cfg(test)]
            if let Some(blocker) = handler_blocker {
                blocker.0.wait();
                blocker.1.wait();
            }
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                agent_knowledge_gateway::serve_ingress_until(
                    &queue_root,
                    &stream,
                    &stream,
                    &handler_deadline,
                )
            }));
            let (outcome, diagnostic) = match result {
                Ok(Ok(())) => (ConnectionOutcome::Served, None),
                Ok(Err(error)) => (ConnectionOutcome::Served, Some(error)),
                Err(_) => (ConnectionOutcome::Panicked, None),
            };
            let _ = sender.send(ConnectionCompletion {
                connection_id,
                outcome,
            });
            if report_diagnostics.load(Ordering::Acquire)
                && let Some(error) = diagnostic
            {
                let _ =
                    diagnostic_sender.try_send(format!("queue ingress connection failed: {error}"));
            }
        })
        .map_err(QueueIngressListenerError::ThreadSpawn)?;
    Ok(ActiveConnection {
        control,
        deadline,
        cancellation_started: None,
        thread,
    })
}

fn expire_connections(active: &mut HashMap<u64, ActiveConnection>, now: Instant) -> Option<u64> {
    let mut stalled = None;
    for (connection_id, connection) in active.iter_mut() {
        if now >= connection.deadline.expires_at() {
            connection.deadline.cancel();
            let _ = connection.control.shutdown(std::net::Shutdown::Both);
            let cancelled_at = connection.cancellation_started.get_or_insert(now);
            if now.duration_since(*cancelled_at) >= HANDLER_CANCELLATION_GRACE {
                stalled.get_or_insert(*connection_id);
            }
        }
    }
    stalled
}

fn drain_completions(
    receiver: &Receiver<ConnectionCompletion>,
    active: &mut HashMap<u64, ActiveConnection>,
) -> Result<(), QueueIngressListenerError> {
    loop {
        match receiver.try_recv() {
            Ok(completion) => finish_connection(completion, active)?,
            Err(TryRecvError::Empty) => return Ok(()),
            Err(TryRecvError::Disconnected) if active.is_empty() => return Ok(()),
            Err(TryRecvError::Disconnected) => {
                return Err(QueueIngressListenerError::CompletionChannelClosed);
            }
        }
    }
}

fn finish_connection(
    completion: ConnectionCompletion,
    active: &mut HashMap<u64, ActiveConnection>,
) -> Result<(), QueueIngressListenerError> {
    let connection = active
        .remove(&completion.connection_id)
        .ok_or(QueueIngressListenerError::UnknownConnection)?;
    connection
        .thread
        .join()
        .map_err(|_| QueueIngressListenerError::ConnectionPanicked)?;
    match completion.outcome {
        ConnectionOutcome::Served => Ok(()),
        ConnectionOutcome::Panicked => Err(QueueIngressListenerError::ConnectionPanicked),
    }
}

struct ActiveConnection {
    control: UnixStream,
    deadline: QueueOperationDeadline,
    cancellation_started: Option<Instant>,
    thread: JoinHandle<()>,
}

struct ConnectionCompletion {
    connection_id: u64,
    outcome: ConnectionOutcome,
}

enum ConnectionOutcome {
    Served,
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
        UnixAddr::new(socket_path)
            .map_err(errno_io)
            .map_err(|source| QueueIngressListenerError::SocketAddressPath {
                path: socket_path.to_path_buf(),
                source,
            })?;
        let parent_path = socket_path
            .parent()
            .ok_or(QueueIngressListenerError::InvalidSocketPath)?;
        let parent = PathAttestation::resolve_destination(parent_path)
            .map_err(QueueIngressListenerError::ParentAttestation)?;
        if parent.path() != parent_path {
            return Err(QueueIngressListenerError::NonCanonicalSocketParent);
        }
        let parent_metadata = fs::metadata(parent.stable_path())
            .map_err(QueueIngressListenerError::ParentInspection)?;
        if !parent_metadata.is_dir() {
            return Err(QueueIngressListenerError::InvalidSocketParent);
        }
        let parent_mode = parent_metadata.mode();
        if parent_metadata.uid() != Uid::effective().as_raw()
            || parent_mode & 0o200 == 0
            || parent_mode & 0o022 != 0
            || parent_mode & 0o2000 == 0
        {
            return Err(QueueIngressListenerError::UnsafeSocketParent);
        }
        validate_socket_namespace(parent.path())?;
        let socket_name = socket_path
            .file_name()
            .ok_or(QueueIngressListenerError::InvalidSocketPath)?;
        if is_reserved_socket_name(socket_name) {
            return Err(QueueIngressListenerError::ReservedSocketName);
        }
        let canonical_socket_path = parent.path().join(socket_name);
        UnixAddr::new(&canonical_socket_path)
            .map_err(errno_io)
            .map_err(|source| QueueIngressListenerError::SocketAddressPath {
                path: canonical_socket_path.clone(),
                source,
            })?;
        let temporary_name = format!("{TEMPORARY_SOCKET_PREFIX}{}", Ulid::generate());
        let configured_temporary = parent.path().join(&temporary_name);
        UnixAddr::new(&configured_temporary)
            .map_err(errno_io)
            .map_err(|source| QueueIngressListenerError::TemporarySocketPath {
                path: configured_temporary.clone(),
                source,
            })?;

        let lock_path = parent.stable_path().join(LISTENER_LOCK_FILE);
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
        let public_name = socket_name.as_bytes().to_vec();
        let previous_state = resolve_legacy_socket_state(
            parent.stable_path(),
            socket_name,
            read_socket_state(parent.stable_path())?,
        )?;
        validate_socket_configuration(parent.stable_path(), socket_name, &previous_state)?;
        recover_recorded_temporary(parent.stable_path(), &previous_state)?;

        let stable_socket_path = parent.stable_path().join(socket_name);
        match fs::symlink_metadata(&stable_socket_path) {
            Ok(metadata) if metadata.file_type().is_socket() => {
                match probe_socket(&canonical_socket_path)? {
                    SocketProbe::Live => return Err(QueueIngressListenerError::AlreadyRunning),
                    SocketProbe::Stale => {
                        let observed = SocketIdentity::from_metadata(&metadata);
                        if !previous_state.owns_public(socket_name, observed) {
                            return Err(QueueIngressListenerError::UnownedStaleSocket);
                        }
                        let current = fs::symlink_metadata(&stable_socket_path)
                            .map_err(QueueIngressListenerError::SocketInspection)?;
                        if !current.file_type().is_socket()
                            || SocketIdentity::from_metadata(&current) != observed
                        {
                            return Err(QueueIngressListenerError::SocketIdentityChanged);
                        }
                        fs::remove_file(&stable_socket_path)
                            .map_err(QueueIngressListenerError::StaleSocketRemoval)?;
                    }
                    SocketProbe::Missing => {}
                }
            }
            Ok(_) => return Err(QueueIngressListenerError::InvalidSocketTarget),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(QueueIngressListenerError::SocketInspection(error)),
        }

        write_socket_state(
            parent.stable_path(),
            &SocketState::Preparing {
                public: public_name.clone(),
                temporary: temporary_name.clone(),
            },
        )?;
        let stable_temporary = parent.stable_path().join(&temporary_name);
        let listener = UnixListener::bind(&configured_temporary).map_err(|source| {
            QueueIngressListenerError::Bind {
                path: configured_temporary.clone(),
                source,
            }
        })?;
        let temporary_node = OwnedSocketNode::capture(stable_temporary.clone())?;
        fs::set_permissions(&stable_temporary, fs::Permissions::from_mode(SOCKET_MODE))
            .map_err(QueueIngressListenerError::SocketPermissions)?;
        write_socket_state(
            parent.stable_path(),
            &SocketState::Prepared {
                public: public_name.clone(),
                temporary: temporary_name.clone(),
                identity: temporary_node.identity(),
            },
        )?;
        let stable_parent = File::open(parent.stable_path())
            .map_err(QueueIngressListenerError::SocketPublication)?;
        renameat2(
            &stable_parent,
            temporary_name.as_str(),
            &stable_parent,
            socket_name,
            RenameFlags::RENAME_NOREPLACE,
        )
        .map_err(errno_io)
        .map_err(QueueIngressListenerError::SocketPublication)?;
        stable_parent
            .sync_all()
            .map_err(QueueIngressListenerError::SocketPublication)?;
        drop(temporary_node);
        let socket_node = OwnedSocketNode::capture(stable_socket_path.clone())?;
        write_socket_state(
            parent.stable_path(),
            &SocketState::Owned {
                public: public_name,
                identity: socket_node.identity(),
            },
        )?;
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

fn is_reserved_socket_name(name: &OsStr) -> bool {
    [
        LISTENER_LOCK_FILE,
        SOCKET_STATE_FILE,
        SOCKET_STATE_TEMPORARY_FILE,
    ]
    .into_iter()
    .any(|reserved| name == OsStr::new(reserved))
}

fn validate_socket_namespace(parent: &Path) -> Result<(), QueueIngressListenerError> {
    let effective_uid = Uid::effective().as_raw();
    for ancestor in parent.ancestors().skip(1) {
        let metadata =
            fs::metadata(ancestor).map_err(QueueIngressListenerError::ParentInspection)?;
        let mode = metadata.mode();
        if (metadata.uid() != 0 && metadata.uid() != effective_uid)
            || (mode & 0o022 != 0 && mode & 0o1000 == 0)
        {
            return Err(QueueIngressListenerError::UnsafeSocketNamespace);
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SocketProbe {
    Live,
    Stale,
    Missing,
}

fn probe_socket(path: &Path) -> Result<SocketProbe, QueueIngressListenerError> {
    let descriptor = socket(
        AddressFamily::Unix,
        SockType::Stream,
        SockFlag::SOCK_CLOEXEC | SockFlag::SOCK_NONBLOCK,
        None,
    )
    .map_err(errno_io)
    .map_err(QueueIngressListenerError::SocketProbe)?;
    let address = UnixAddr::new(path)
        .map_err(errno_io)
        .map_err(QueueIngressListenerError::SocketProbe)?;
    match connect(descriptor.as_raw_fd(), &address) {
        Ok(()) => Ok(SocketProbe::Live),
        Err(Errno::ECONNREFUSED) => Ok(SocketProbe::Stale),
        Err(Errno::ENOENT) => Ok(SocketProbe::Missing),
        Err(Errno::EAGAIN | Errno::EINPROGRESS | Errno::EALREADY) => Ok(SocketProbe::Live),
        Err(error) => Err(QueueIngressListenerError::SocketProbe(errno_io(error))),
    }
}

fn errno_io(error: Errno) -> io::Error {
    io::Error::from_raw_os_error(error as i32)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SocketIdentity {
    device: u64,
    inode: u64,
}

impl SocketIdentity {
    fn from_metadata(metadata: &fs::Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SocketState {
    Absent,
    LegacyPreparing {
        temporary: String,
    },
    LegacyPrepared {
        temporary: String,
        identity: SocketIdentity,
    },
    LegacyOwned {
        identity: SocketIdentity,
    },
    Preparing {
        public: Vec<u8>,
        temporary: String,
    },
    Prepared {
        public: Vec<u8>,
        temporary: String,
        identity: SocketIdentity,
    },
    Owned {
        public: Vec<u8>,
        identity: SocketIdentity,
    },
}

impl SocketState {
    fn owns_public(&self, public: &OsStr, identity: SocketIdentity) -> bool {
        matches!(
            self,
            Self::Prepared {
                public: owned_public,
                identity: owned,
                ..
            } | Self::Owned {
                public: owned_public,
                identity: owned,
            } if owned_public == public.as_bytes() && *owned == identity
        )
    }

    fn public(&self) -> Option<&[u8]> {
        match self {
            Self::Preparing { public, .. }
            | Self::Prepared { public, .. }
            | Self::Owned { public, .. } => Some(public),
            Self::Absent
            | Self::LegacyPreparing { .. }
            | Self::LegacyPrepared { .. }
            | Self::LegacyOwned { .. } => None,
        }
    }

    fn temporary(&self) -> Option<(&str, Option<SocketIdentity>)> {
        match self {
            Self::Preparing { temporary, .. } => Some((temporary, None)),
            Self::Prepared {
                temporary,
                identity,
                ..
            } => Some((temporary, Some(*identity))),
            Self::Absent
            | Self::LegacyPreparing { .. }
            | Self::LegacyPrepared { .. }
            | Self::LegacyOwned { .. }
            | Self::Owned { .. } => None,
        }
    }
}

fn resolve_legacy_socket_state(
    stable_parent: &Path,
    configured_public: &OsStr,
    state: SocketState,
) -> Result<SocketState, QueueIngressListenerError> {
    match state {
        SocketState::LegacyPreparing { temporary } => Ok(SocketState::Preparing {
            public: configured_public.as_bytes().to_vec(),
            temporary,
        }),
        SocketState::LegacyPrepared {
            temporary,
            identity,
        } => {
            let located = locate_recorded_socket(stable_parent, identity)?;
            let public = match located {
                Some(name) if name.as_bytes() != temporary.as_bytes() => name.into_vec(),
                Some(_) | None => configured_public.as_bytes().to_vec(),
            };
            Ok(SocketState::Prepared {
                public,
                temporary,
                identity,
            })
        }
        SocketState::LegacyOwned { identity } => {
            let public = locate_recorded_socket(stable_parent, identity)?
                .map_or_else(|| configured_public.as_bytes().to_vec(), OsString::into_vec);
            Ok(SocketState::Owned { public, identity })
        }
        state => Ok(state),
    }
}

fn locate_recorded_socket(
    stable_parent: &Path,
    identity: SocketIdentity,
) -> Result<Option<OsString>, QueueIngressListenerError> {
    let entries = fs::read_dir(stable_parent)
        .map_err(QueueIngressListenerError::SocketDirectoryInspection)?;
    let mut located = None;
    for (index, entry) in entries.enumerate() {
        if index >= MAXIMUM_SOCKET_DIRECTORY_ENTRIES {
            return Err(QueueIngressListenerError::SocketDirectoryLimitExceeded);
        }
        let entry = entry.map_err(QueueIngressListenerError::SocketDirectoryInspection)?;
        let metadata = fs::symlink_metadata(entry.path())
            .map_err(QueueIngressListenerError::SocketDirectoryInspection)?;
        if !metadata.file_type().is_socket() || SocketIdentity::from_metadata(&metadata) != identity
        {
            continue;
        }
        if located.is_some() {
            return Err(QueueIngressListenerError::AmbiguousLegacySocketState);
        }
        located = Some(entry.file_name());
    }
    Ok(located)
}

fn validate_socket_configuration(
    stable_parent: &Path,
    configured_public: &OsStr,
    state: &SocketState,
) -> Result<(), QueueIngressListenerError> {
    let Some(recorded_public) = state.public() else {
        return Ok(());
    };
    if recorded_public == configured_public.as_bytes() {
        return Ok(());
    }
    let recorded_path = stable_parent.join(OsStr::from_bytes(recorded_public));
    let public_absent = match fs::symlink_metadata(&recorded_path) {
        Ok(_) => false,
        Err(error) if error.kind() == io::ErrorKind::NotFound => true,
        Err(error) => return Err(QueueIngressListenerError::SocketInspection(error)),
    };
    let temporary_absent = match state.temporary() {
        Some((temporary, _)) => match fs::symlink_metadata(stable_parent.join(temporary)) {
            Ok(_) => false,
            Err(error) if error.kind() == io::ErrorKind::NotFound => true,
            Err(error) => return Err(QueueIngressListenerError::SocketInspection(error)),
        },
        None => true,
    };
    if public_absent && temporary_absent {
        return Ok(());
    }
    Err(QueueIngressListenerError::SocketConfigurationChanged {
        recorded: OsString::from_vec(recorded_public.to_vec()),
        configured: configured_public.to_os_string(),
    })
}

fn recover_recorded_temporary(
    stable_parent: &Path,
    state: &SocketState,
) -> Result<(), QueueIngressListenerError> {
    let Some((temporary, expected_identity)) = state.temporary() else {
        return Ok(());
    };
    let path = stable_parent.join(temporary);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(QueueIngressListenerError::SocketInspection(error)),
    };
    if !metadata.file_type().is_socket() {
        return Err(QueueIngressListenerError::InvalidSocketTarget);
    }
    let observed = SocketIdentity::from_metadata(&metadata);
    if expected_identity.is_some_and(|expected| expected != observed) {
        return Err(QueueIngressListenerError::SocketIdentityChanged);
    }
    if probe_socket(&path)? == SocketProbe::Live {
        return Err(QueueIngressListenerError::AlreadyRunning);
    }
    let current =
        fs::symlink_metadata(&path).map_err(QueueIngressListenerError::SocketInspection)?;
    if !current.file_type().is_socket() || SocketIdentity::from_metadata(&current) != observed {
        return Err(QueueIngressListenerError::SocketIdentityChanged);
    }
    fs::remove_file(path).map_err(QueueIngressListenerError::StaleSocketRemoval)
}

fn read_socket_state(stable_parent: &Path) -> Result<SocketState, QueueIngressListenerError> {
    let path = stable_parent.join(SOCKET_STATE_FILE);
    let file = match OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_NOFOLLOW)
        .open(&path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(SocketState::Absent),
        Err(error) => return Err(QueueIngressListenerError::StateOpen(error)),
    };
    let metadata = file
        .metadata()
        .map_err(QueueIngressListenerError::StateInspection)?;
    if !metadata.is_file() || metadata.nlink() != 1 || metadata.len() > MAXIMUM_SOCKET_STATE_BYTES {
        return Err(QueueIngressListenerError::InvalidStateFile);
    }
    let mut contents = String::new();
    file.take(MAXIMUM_SOCKET_STATE_BYTES)
        .read_to_string(&mut contents)
        .map_err(QueueIngressListenerError::StateContents)?;
    if contents == format!("{LEGACY_SOCKET_STATE_FORMAT} preparing\n") {
        return Ok(SocketState::Absent);
    }
    let line = contents
        .strip_suffix('\n')
        .filter(|line| !line.contains(['\n', '\r']))
        .ok_or(QueueIngressListenerError::InvalidStateContents)?;
    let mut fields = line.split(' ');
    let format = fields.next();
    let status = fields.next();
    let legacy = match format {
        Some(SOCKET_STATE_FORMAT) => false,
        Some(LEGACY_SOCKET_STATE_FORMAT) => true,
        _ => return Err(QueueIngressListenerError::InvalidStateContents),
    };
    if status.is_none() {
        return Err(QueueIngressListenerError::InvalidStateContents);
    }
    let parse_identity = |device: Option<&str>, inode: Option<&str>| {
        Some(SocketIdentity {
            device: device?.parse().ok()?,
            inode: inode?.parse().ok()?,
        })
    };
    let state = if legacy {
        match status {
            Some("preparing") => SocketState::LegacyPreparing {
                temporary: validate_temporary_name(fields.next())?,
            },
            Some("prepared") => SocketState::LegacyPrepared {
                temporary: validate_temporary_name(fields.next())?,
                identity: parse_identity(fields.next(), fields.next())
                    .ok_or(QueueIngressListenerError::InvalidStateContents)?,
            },
            Some("owned") => SocketState::LegacyOwned {
                identity: parse_identity(fields.next(), fields.next())
                    .ok_or(QueueIngressListenerError::InvalidStateContents)?,
            },
            _ => return Err(QueueIngressListenerError::InvalidStateContents),
        }
    } else {
        let public = decode_public_name(fields.next())?;
        match status {
            Some("preparing") => SocketState::Preparing {
                public,
                temporary: validate_temporary_name(fields.next())?,
            },
            Some("prepared") => SocketState::Prepared {
                public,
                temporary: validate_temporary_name(fields.next())?,
                identity: parse_identity(fields.next(), fields.next())
                    .ok_or(QueueIngressListenerError::InvalidStateContents)?,
            },
            Some("owned") => SocketState::Owned {
                public,
                identity: parse_identity(fields.next(), fields.next())
                    .ok_or(QueueIngressListenerError::InvalidStateContents)?,
            },
            _ => return Err(QueueIngressListenerError::InvalidStateContents),
        }
    };
    if fields.next().is_some() {
        return Err(QueueIngressListenerError::InvalidStateContents);
    }
    Ok(state)
}

fn decode_public_name(value: Option<&str>) -> Result<Vec<u8>, QueueIngressListenerError> {
    let encoded = value.ok_or(QueueIngressListenerError::InvalidStateContents)?;
    if encoded.is_empty() || encoded.len() % 2 != 0 {
        return Err(QueueIngressListenerError::InvalidStateContents);
    }
    let mut decoded = Vec::with_capacity(encoded.len() / 2);
    for pair in encoded.as_bytes().chunks_exact(2) {
        let high = decode_hex_digit(pair[0])?;
        let low = decode_hex_digit(pair[1])?;
        decoded.push((high << 4) | low);
    }
    if decoded.contains(&0)
        || decoded.contains(&b'/')
        || decoded.as_slice() == b"."
        || decoded.as_slice() == b".."
    {
        return Err(QueueIngressListenerError::InvalidStateContents);
    }
    Ok(decoded)
}

fn decode_hex_digit(value: u8) -> Result<u8, QueueIngressListenerError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(QueueIngressListenerError::InvalidStateContents),
    }
}

fn encode_public_name(public: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(public.len() * 2);
    for byte in public {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn validate_temporary_name(value: Option<&str>) -> Result<String, QueueIngressListenerError> {
    let value = value.ok_or(QueueIngressListenerError::InvalidStateContents)?;
    let token = value
        .strip_prefix(TEMPORARY_SOCKET_PREFIX)
        .ok_or(QueueIngressListenerError::InvalidStateContents)?;
    token
        .parse::<Ulid>()
        .map_err(|_| QueueIngressListenerError::InvalidStateContents)?;
    Ok(value.into())
}

fn write_socket_state(
    stable_parent: &Path,
    state: &SocketState,
) -> Result<(), QueueIngressListenerError> {
    let temporary = stable_parent.join(SOCKET_STATE_TEMPORARY_FILE);
    let final_path = stable_parent.join(SOCKET_STATE_FILE);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_NOFOLLOW)
        .open(&temporary)
        .map_err(QueueIngressListenerError::StateOpen)?;
    let metadata = file
        .metadata()
        .map_err(QueueIngressListenerError::StateInspection)?;
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(QueueIngressListenerError::InvalidStateFile);
    }
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(QueueIngressListenerError::StatePermissions)?;
    match state {
        SocketState::Preparing { public, temporary } => {
            let public = encode_public_name(public);
            writeln!(file, "{SOCKET_STATE_FORMAT} preparing {public} {temporary}")
        }
        SocketState::Prepared {
            public,
            temporary,
            identity,
        } => {
            let public = encode_public_name(public);
            writeln!(
                file,
                "{SOCKET_STATE_FORMAT} prepared {public} {temporary} {} {}",
                identity.device, identity.inode
            )
        }
        SocketState::Owned { public, identity } => {
            let public = encode_public_name(public);
            writeln!(
                file,
                "{SOCKET_STATE_FORMAT} owned {public} {} {}",
                identity.device, identity.inode
            )
        }
        SocketState::Absent
        | SocketState::LegacyPreparing { .. }
        | SocketState::LegacyPrepared { .. }
        | SocketState::LegacyOwned { .. } => {
            return Err(QueueIngressListenerError::InvalidStateContents);
        }
    }
    .map_err(QueueIngressListenerError::StateContents)?;
    file.sync_all()
        .map_err(QueueIngressListenerError::StateContents)?;
    drop(file);
    fs::rename(&temporary, &final_path).map_err(QueueIngressListenerError::StatePublication)?;
    File::open(stable_parent)
        .and_then(|directory| directory.sync_all())
        .map_err(QueueIngressListenerError::StatePublication)
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

    const fn identity(&self) -> SocketIdentity {
        SocketIdentity {
            device: self.device,
            inode: self.inode,
        }
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
    Identity(RuntimeIdentityError),
    SignalRegistration(io::Error),
    Listener(QueueIngressListenerError),
}

impl fmt::Display for QueueIngressCommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity(error) => {
                write!(
                    formatter,
                    "Queue Ingress identity validation failed: {error}"
                )
            }
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
            Self::Identity(error) => Some(error),
            Self::SignalRegistration(error) => Some(error),
            Self::Listener(error) => Some(error),
        }
    }
}

#[derive(Debug)]
pub(crate) enum QueueIngressListenerError {
    InvalidSocketPath,
    ReservedSocketName,
    SocketAddressPath {
        path: PathBuf,
        source: io::Error,
    },
    ParentAttestation(PathAttestationError),
    NonCanonicalSocketParent,
    ParentInspection(io::Error),
    InvalidSocketParent,
    UnsafeSocketParent,
    UnsafeSocketNamespace,
    TemporarySocketPath {
        path: PathBuf,
        source: io::Error,
    },
    LockOpen(io::Error),
    LockInspection(io::Error),
    InvalidLockFile,
    LockPermissions(io::Error),
    StateOpen(io::Error),
    StateInspection(io::Error),
    StatePermissions(io::Error),
    StateContents(io::Error),
    StatePublication(io::Error),
    InvalidStateFile,
    InvalidStateContents,
    SocketConfigurationChanged {
        recorded: OsString,
        configured: OsString,
    },
    AlreadyRunning,
    Lock(io::Error),
    SocketInspection(io::Error),
    SocketProbe(io::Error),
    InvalidSocketTarget,
    UnownedStaleSocket,
    SocketIdentityChanged,
    SocketDirectoryInspection(io::Error),
    SocketDirectoryLimitExceeded,
    AmbiguousLegacySocketState,
    StaleSocketRemoval(io::Error),
    SocketPublication(io::Error),
    Bind {
        path: PathBuf,
        source: io::Error,
    },
    SocketPermissions(io::Error),
    Nonblocking(io::Error),
    Accept(io::Error),
    ConnectionConfiguration(io::Error),
    InvalidConnectionTimeout,
    ThreadSpawn(io::Error),
    CompletionChannelClosed,
    UnknownConnection,
    ConnectionCancellationTimedOut(u64),
    ConnectionPanicked,
}

impl fmt::Display for QueueIngressListenerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSocketPath => {
                formatter.write_str("queue ingress socket path must be an absolute file path")
            }
            Self::ReservedSocketName => formatter.write_str(
                "queue ingress socket name conflicts with reserved listener state",
            ),
            Self::SocketAddressPath { path, source } => write!(
                formatter,
                "queue ingress socket path {} is not addressable: {source}",
                path.display()
            ),
            Self::ParentAttestation(error) => {
                write!(
                    formatter,
                    "queue ingress socket parent attestation failed: {error}"
                )
            }
            Self::NonCanonicalSocketParent => formatter.write_str(
                "queue ingress socket parent must be canonical and contain no symbolic links",
            ),
            Self::ParentInspection(error) => {
                write!(
                    formatter,
                    "could not inspect queue ingress socket parent: {error}"
                )
            }
            Self::InvalidSocketParent => {
                formatter.write_str("queue ingress socket parent is not a directory")
            }
            Self::UnsafeSocketParent => formatter.write_str(
                "queue ingress socket parent must be owner-writable, setgid, and not writable by group or other",
            ),
            Self::UnsafeSocketNamespace => formatter.write_str(
                "queue ingress socket ancestors must be owned by root or the listener identity, with sticky protection for writable namespaces",
            ),
            Self::TemporarySocketPath { path, source } => write!(
                formatter,
                "queue ingress runtime path leaves no room for temporary socket {}: {source}",
                path.display()
            ),
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
            Self::StateOpen(error) => {
                write!(formatter, "could not open queue ingress socket state: {error}")
            }
            Self::StateInspection(error) => {
                write!(formatter, "could not inspect queue ingress socket state: {error}")
            }
            Self::StatePermissions(error) => write!(
                formatter,
                "could not restrict queue ingress socket state: {error}"
            ),
            Self::StateContents(error) => {
                write!(formatter, "could not write queue ingress socket state: {error}")
            }
            Self::StatePublication(error) => write!(
                formatter,
                "could not publish queue ingress socket state: {error}"
            ),
            Self::InvalidStateFile => {
                formatter.write_str("queue ingress socket state is not a private regular file")
            }
            Self::InvalidStateContents => {
                formatter.write_str("queue ingress socket state is malformed")
            }
            Self::SocketConfigurationChanged {
                recorded,
                configured,
            } => write!(
                formatter,
                "queue ingress socket name changed from {} to {} while the prior publication still exists",
                recorded.to_string_lossy(),
                configured.to_string_lossy()
            ),
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
            Self::UnownedStaleSocket => formatter.write_str(
                "queue ingress socket is stale but was not published by this listener",
            ),
            Self::SocketIdentityChanged => {
                formatter.write_str("queue ingress socket identity changed during recovery")
            }
            Self::SocketDirectoryInspection(error) => write!(
                formatter,
                "could not inspect queue ingress runtime directory: {error}"
            ),
            Self::SocketDirectoryLimitExceeded => formatter.write_str(
                "queue ingress runtime directory exceeds the legacy recovery scan limit",
            ),
            Self::AmbiguousLegacySocketState => formatter.write_str(
                "legacy queue ingress state matches multiple sockets in the runtime directory",
            ),
            Self::StaleSocketRemoval(error) => {
                write!(
                    formatter,
                    "could not remove stale queue ingress socket: {error}"
                )
            }
            Self::SocketPublication(error) => {
                write!(formatter, "could not publish queue ingress socket: {error}")
            }
            Self::Bind { path, source } => {
                write!(formatter, "could not bind queue ingress socket {}: {source}", path.display())
            }
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
                    "could not start queue ingress listener thread: {error}"
                )
            }
            Self::CompletionChannelClosed => {
                formatter.write_str("queue ingress connection completion channel closed")
            }
            Self::UnknownConnection => {
                formatter.write_str("queue ingress completed an unknown connection")
            }
            Self::ConnectionCancellationTimedOut(connection_id) => write!(
                formatter,
                "queue ingress connection {connection_id} did not stop after cancellation"
            ),
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
            Self::SocketAddressPath { source: error, .. }
            | Self::ParentInspection(error)
            | Self::TemporarySocketPath { source: error, .. }
            | Self::LockOpen(error)
            | Self::LockInspection(error)
            | Self::LockPermissions(error)
            | Self::StateOpen(error)
            | Self::StateInspection(error)
            | Self::StatePermissions(error)
            | Self::StateContents(error)
            | Self::StatePublication(error)
            | Self::Lock(error)
            | Self::SocketInspection(error)
            | Self::SocketDirectoryInspection(error)
            | Self::SocketProbe(error)
            | Self::StaleSocketRemoval(error)
            | Self::SocketPublication(error)
            | Self::Bind { source: error, .. }
            | Self::SocketPermissions(error)
            | Self::Nonblocking(error)
            | Self::Accept(error)
            | Self::ConnectionConfiguration(error)
            | Self::ThreadSpawn(error) => Some(error),
            Self::InvalidSocketPath
            | Self::ReservedSocketName
            | Self::NonCanonicalSocketParent
            | Self::InvalidSocketParent
            | Self::UnsafeSocketParent
            | Self::UnsafeSocketNamespace
            | Self::InvalidLockFile
            | Self::InvalidStateFile
            | Self::InvalidStateContents
            | Self::SocketConfigurationChanged { .. }
            | Self::AlreadyRunning
            | Self::InvalidSocketTarget
            | Self::UnownedStaleSocket
            | Self::SocketIdentityChanged
            | Self::SocketDirectoryLimitExceeded
            | Self::AmbiguousLegacySocketState
            | Self::InvalidConnectionTimeout
            | Self::CompletionChannelClosed
            | Self::UnknownConnection
            | Self::ConnectionCancellationTimedOut(_)
            | Self::ConnectionPanicked => None,
        }
    }
}

#[cfg(test)]
#[path = "queue_ingress/tests.rs"]
mod tests;

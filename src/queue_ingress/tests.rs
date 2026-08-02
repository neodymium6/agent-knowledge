use std::fs;
use std::io::{self, Cursor, Write};
use std::num::NonZeroUsize;
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::symlink;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use agent_knowledge_gateway::{IngressClient, IngressClientError};
use agent_knowledge_protocol::{ClientId, StatusRequest};
use agent_knowledge_queue::{FileQueue, PackagePolicy};
use nix::errno::Errno;
use nix::sys::socket::{
    AddressFamily, Backlog, SockFlag, SockType, UnixAddr, bind as socket_bind, connect,
    listen as socket_listen, socket,
};
use tar::{Builder, EntryType, Header};

use super::{
    ListenSettings, PublishedSocket, QueueIngressListenerError, SOCKET_MODE, SocketIdentity,
    SocketProbe, SocketState, listen_until, probe_socket, write_socket_state,
};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

#[derive(Clone, Default)]
struct SharedDiagnosticOutput(Arc<Mutex<Vec<u8>>>);

impl SharedDiagnosticOutput {
    fn contents(&self) -> Vec<u8> {
        self.0
            .lock()
            .unwrap_or_else(|error| panic!("diagnostic output must not be poisoned: {error}"))
            .clone()
    }
}

impl Write for SharedDiagnosticOutput {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| io::Error::other("diagnostic output lock poisoned"))?
            .write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct BlockingDiagnosticOutput {
    blocker: Arc<(std::sync::Barrier, std::sync::Barrier)>,
    blocked: Arc<AtomicBool>,
}

impl Write for BlockingDiagnosticOutput {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if !self.blocked.swap(true, Ordering::AcqRel) {
            self.blocker.0.wait();
            self.blocker.1.wait();
        }
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl TestDirectory {
    fn create() -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "agent-knowledge-listener-test-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path)
            .unwrap_or_else(|error| panic!("listener test directory must be created: {error}"));
        fs::set_permissions(&path, fs::Permissions::from_mode(0o2750))
            .unwrap_or_else(|error| panic!("listener test directory mode must be set: {error}"));
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.0)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            panic!("listener test directory must be removed: {error}");
        }
    }
}

fn settings(root: &Path) -> ListenSettings {
    let queue_root = root.join("queue");
    FileQueue::initialize(&queue_root, PackagePolicy::default())
        .unwrap_or_else(|error| panic!("test queue must be initialized: {error}"));
    ListenSettings {
        queue_root,
        socket_path: root.join("run/queue-ingress.sock"),
        maximum_connections: NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
        connection_timeout: Duration::from_secs(5),
        deadline_observer: None,
        handler_blocker: None,
    }
}

fn wait_for_socket(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "listener socket was not published"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn valid_archive() -> Vec<u8> {
    const REQUEST: &[u8] = br#"{
  "protocol_version": 1,
  "request_id": "01K00000000000000000000000",
  "title": "Record a fictional listener test",
  "project": "fictional-project",
  "document_type": "experiment",
  "created_at": "2026-07-31T03:50:00Z",
  "operations": [{
    "type": "create_document",
    "document_id": "01K00000000000000000000001",
    "content": "run/index.md"
  }]
}"#;
    const MARKDOWN: &[u8] = b"---\n\
schema_version: 1\n\
document_id: 01K00000000000000000000001\n\
title: Fictional listener test\n\
created: 2026-07-31T03:50:00Z\n\
request_id: 01K00000000000000000000000\n\
status: active\n\
---\n\
Fictional listener body.\n";
    fn append(builder: &mut Builder<Vec<u8>>, path: &str, contents: &[u8]) {
        let mut header = Header::new_gnu();
        header.set_entry_type(EntryType::Regular);
        header.set_mode(0o644);
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(0);
        header.set_size(contents.len() as u64);
        header.set_cksum();
        builder
            .append_data(&mut header, path, Cursor::new(contents))
            .unwrap_or_else(|error| panic!("listener tar entry must append: {error}"));
    }
    let mut builder = Builder::new(Vec::new());
    append(&mut builder, "request.json", REQUEST);
    append(&mut builder, "payload/run/index.md", MARKDOWN);
    builder
        .into_inner()
        .unwrap_or_else(|error| panic!("listener tar fixture must finish: {error}"))
}

#[test]
fn serves_clients_until_shutdown_and_removes_the_socket() {
    let root = TestDirectory::create();
    fs::create_dir(root.path().join("run"))
        .unwrap_or_else(|error| panic!("runtime directory must be created: {error}"));
    fs::set_permissions(root.path().join("run"), fs::Permissions::from_mode(0o2750))
        .unwrap_or_else(|error| panic!("runtime directory mode must be set: {error}"));
    let settings = settings(root.path());
    let socket_path = settings.socket_path.clone();
    let stopping = Arc::new(AtomicBool::new(false));
    let listener_stopping = Arc::clone(&stopping);
    let diagnostic_output = SharedDiagnosticOutput::default();
    let captured_output = diagnostic_output.clone();
    let listener = thread::spawn(move || {
        let result = listen_until(settings, diagnostic_output, || {
            listener_stopping.load(Ordering::Relaxed)
        });
        (result, captured_output.contents())
    });

    wait_for_socket(&socket_path);
    let mode = fs::symlink_metadata(&socket_path)
        .unwrap_or_else(|error| panic!("listener socket must exist: {error}"))
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, SOCKET_MODE);
    let request = StatusRequest::new(
        "01K00000000000000000000000"
            .parse()
            .unwrap_or_else(|error| panic!("request ID must parse: {error}")),
    );
    let error = match IngressClient::new(&socket_path).status(request, Duration::from_secs(2)) {
        Ok(_) => panic!("unknown request must be rejected"),
        Err(error) => error,
    };
    assert!(matches!(error, IngressClientError::Broker(_)));

    let stalled = UnixStream::connect(&socket_path)
        .unwrap_or_else(|error| panic!("stalled test connection must open: {error}"));
    stopping.store(true, Ordering::Relaxed);
    let (result, output) = listener
        .join()
        .unwrap_or_else(|_| panic!("listener thread must not panic"));
    drop(stalled);
    result.unwrap_or_else(|error| panic!("listener must stop cleanly: {error}"));
    assert!(!socket_path.exists());
    assert!(
        String::from_utf8(output)
            .unwrap_or_else(|error| panic!("diagnostics must be UTF-8: {error}"))
            .contains("queue ingress connection failed")
    );
}

#[test]
fn absolute_connection_timeout_releases_the_concurrency_slot() {
    let root = TestDirectory::create();
    fs::create_dir(root.path().join("run"))
        .unwrap_or_else(|error| panic!("runtime directory must be created: {error}"));
    fs::set_permissions(root.path().join("run"), fs::Permissions::from_mode(0o2750))
        .unwrap_or_else(|error| panic!("runtime directory mode must be set: {error}"));
    let mut settings = settings(root.path());
    settings.maximum_connections = NonZeroUsize::MIN;
    settings.connection_timeout = Duration::from_millis(150);
    let socket_path = settings.socket_path.clone();
    let stopping = Arc::new(AtomicBool::new(false));
    let listener_stopping = Arc::clone(&stopping);
    let listener = thread::spawn(move || {
        listen_until(settings, Vec::new(), || {
            listener_stopping.load(Ordering::Relaxed)
        })
    });

    wait_for_socket(&socket_path);
    let stalled = UnixStream::connect(&socket_path)
        .unwrap_or_else(|error| panic!("stalled test connection must open: {error}"));
    thread::sleep(Duration::from_millis(75));
    let request = StatusRequest::new(
        "01K00000000000000000000000"
            .parse()
            .unwrap_or_else(|error| panic!("request ID must parse: {error}")),
    );
    let error = match IngressClient::new(&socket_path).status(request, Duration::from_secs(2)) {
        Ok(_) => panic!("unknown request must be rejected"),
        Err(error) => error,
    };
    assert!(matches!(error, IngressClientError::Broker(_)));

    stopping.store(true, Ordering::Relaxed);
    drop(stalled);
    listener
        .join()
        .unwrap_or_else(|_| panic!("listener thread must not panic"))
        .unwrap_or_else(|error| panic!("listener must stop cleanly: {error}"));
}

#[test]
fn unresponsive_expired_handler_terminates_the_listener() {
    let root = TestDirectory::create();
    fs::create_dir(root.path().join("run"))
        .unwrap_or_else(|error| panic!("runtime directory must be created: {error}"));
    fs::set_permissions(root.path().join("run"), fs::Permissions::from_mode(0o2750))
        .unwrap_or_else(|error| panic!("runtime directory mode must be set: {error}"));
    let mut settings = settings(root.path());
    settings.connection_timeout = Duration::from_millis(50);
    let blocker = Arc::new((std::sync::Barrier::new(2), std::sync::Barrier::new(2)));
    settings.handler_blocker = Some(Arc::clone(&blocker));
    let socket_path = settings.socket_path.clone();
    let (result_sender, result_receiver) = std::sync::mpsc::sync_channel(1);
    let listener = thread::spawn(move || {
        let _ = result_sender.send(listen_until(settings, Vec::new(), || false));
    });
    wait_for_socket(&socket_path);
    let stalled = UnixStream::connect(&socket_path)
        .unwrap_or_else(|error| panic!("blocked handler connection must open: {error}"));
    blocker.0.wait();

    let result = result_receiver
        .recv_timeout(Duration::from_secs(3))
        .unwrap_or_else(|error| panic!("stalled listener must fail within its bound: {error}"));
    assert!(matches!(
        result,
        Err(QueueIngressListenerError::ConnectionCancellationTimedOut(_))
    ));
    assert!(!socket_path.exists());
    blocker.1.wait();
    drop(stalled);
    listener
        .join()
        .unwrap_or_else(|_| panic!("listener thread must not panic"));
}

#[test]
fn shutdown_cancels_a_handler_waiting_for_the_queue_lock() {
    let root = TestDirectory::create();
    fs::create_dir(root.path().join("run"))
        .unwrap_or_else(|error| panic!("runtime directory must be created: {error}"));
    fs::set_permissions(root.path().join("run"), fs::Permissions::from_mode(0o2750))
        .unwrap_or_else(|error| panic!("runtime directory mode must be set: {error}"));
    let mut settings = settings(root.path());
    settings.connection_timeout = Duration::from_secs(30);
    let (deadline_sender, deadline_receiver) = std::sync::mpsc::channel();
    settings.deadline_observer = Some(deadline_sender);
    let queue_lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(root.path().join("queue/.locks/queue.lock"))
        .unwrap_or_else(|error| panic!("queue lock fixture must open: {error}"));
    queue_lock
        .lock()
        .unwrap_or_else(|error| panic!("queue lock fixture must be held: {error}"));
    let socket_path = settings.socket_path.clone();
    let stopping = Arc::new(AtomicBool::new(false));
    let listener_stopping = Arc::clone(&stopping);
    let listener = thread::spawn(move || {
        listen_until(settings, Vec::new(), || {
            listener_stopping.load(Ordering::Relaxed)
        })
    });
    wait_for_socket(&socket_path);
    let client = thread::spawn(move || {
        let client_id: ClientId = "fictional-node-a"
            .parse()
            .unwrap_or_else(|error| panic!("client fixture must parse: {error}"));
        IngressClient::new(&socket_path).submit(
            client_id,
            Cursor::new(valid_archive()),
            Duration::from_secs(5),
        )
    });

    let operation_deadline = deadline_receiver
        .recv_timeout(Duration::from_secs(2))
        .unwrap_or_else(|error| panic!("handler deadline must be observed: {error}"));
    let observation_deadline = Instant::now() + Duration::from_secs(2);
    while !operation_deadline.lock_wait_observed() {
        assert!(
            Instant::now() < observation_deadline,
            "handler must reach the contended queue lock"
        );
        thread::sleep(Duration::from_millis(10));
    }
    let shutdown_started = Instant::now();
    stopping.store(true, Ordering::Relaxed);
    listener
        .join()
        .unwrap_or_else(|_| panic!("listener thread must not panic"))
        .unwrap_or_else(|error| panic!("listener must stop cleanly: {error}"));
    assert!(shutdown_started.elapsed() < Duration::from_secs(2));
    let _ = client
        .join()
        .unwrap_or_else(|_| panic!("client thread must not panic"));
    drop(queue_lock);
}

#[test]
fn shutdown_detaches_a_handler_that_cannot_observe_cancellation() {
    let root = TestDirectory::create();
    fs::create_dir(root.path().join("run"))
        .unwrap_or_else(|error| panic!("runtime directory must be created: {error}"));
    fs::set_permissions(root.path().join("run"), fs::Permissions::from_mode(0o2750))
        .unwrap_or_else(|error| panic!("runtime directory mode must be set: {error}"));
    let mut settings = settings(root.path());
    settings.connection_timeout = Duration::from_secs(30);
    let blocker = Arc::new((std::sync::Barrier::new(2), std::sync::Barrier::new(2)));
    settings.handler_blocker = Some(Arc::clone(&blocker));
    let socket_path = settings.socket_path.clone();
    let stopping = Arc::new(AtomicBool::new(false));
    let listener_stopping = Arc::clone(&stopping);
    let listener = thread::spawn(move || {
        listen_until(settings, Vec::new(), || {
            listener_stopping.load(Ordering::Relaxed)
        })
    });
    wait_for_socket(&socket_path);
    let stalled = UnixStream::connect(&socket_path)
        .unwrap_or_else(|error| panic!("blocked handler connection must open: {error}"));
    blocker.0.wait();

    let shutdown_started = Instant::now();
    stopping.store(true, Ordering::Relaxed);
    listener
        .join()
        .unwrap_or_else(|_| panic!("listener thread must not panic"))
        .unwrap_or_else(|error| panic!("listener must stop after its grace period: {error}"));
    assert!(shutdown_started.elapsed() < Duration::from_secs(2));
    blocker.1.wait();
    drop(stalled);
}

#[test]
fn blocking_diagnostics_do_not_block_listener_shutdown() {
    let root = TestDirectory::create();
    fs::create_dir(root.path().join("run"))
        .unwrap_or_else(|error| panic!("runtime directory must be created: {error}"));
    fs::set_permissions(root.path().join("run"), fs::Permissions::from_mode(0o2750))
        .unwrap_or_else(|error| panic!("runtime directory mode must be set: {error}"));
    let settings = settings(root.path());
    let socket_path = settings.socket_path.clone();
    let blocker = Arc::new((std::sync::Barrier::new(2), std::sync::Barrier::new(2)));
    let output = BlockingDiagnosticOutput {
        blocker: Arc::clone(&blocker),
        blocked: Arc::new(AtomicBool::new(false)),
    };
    let stopping = Arc::new(AtomicBool::new(false));
    let listener_stopping = Arc::clone(&stopping);
    let listener = thread::spawn(move || {
        listen_until(settings, output, || {
            listener_stopping.load(Ordering::Relaxed)
        })
    });
    wait_for_socket(&socket_path);
    let failing = UnixStream::connect(&socket_path)
        .unwrap_or_else(|error| panic!("failing diagnostic connection must open: {error}"));
    drop(failing);
    blocker.0.wait();
    let request = StatusRequest::new(
        "01K00000000000000000000000"
            .parse()
            .unwrap_or_else(|error| panic!("request ID must parse: {error}")),
    );
    let error = match IngressClient::new(&socket_path).status(request, Duration::from_secs(2)) {
        Ok(_) => panic!("unknown request must be rejected"),
        Err(error) => error,
    };
    assert!(matches!(error, IngressClientError::Broker(_)));

    let shutdown_started = Instant::now();
    stopping.store(true, Ordering::Relaxed);
    listener
        .join()
        .unwrap_or_else(|_| panic!("listener thread must not panic"))
        .unwrap_or_else(|error| panic!("listener must detach a blocked diagnostic: {error}"));
    assert!(shutdown_started.elapsed() < Duration::from_secs(2));
    blocker.1.wait();
}

#[test]
fn refuses_to_replace_a_non_socket_target() {
    let root = TestDirectory::create();
    let socket_path = root.path().join("queue-ingress.sock");
    fs::write(&socket_path, b"fictional sentinel")
        .unwrap_or_else(|error| panic!("sentinel must be written: {error}"));

    let error = match PublishedSocket::bind(&socket_path) {
        Ok(_) => panic!("regular file must not be replaced by the listener"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        QueueIngressListenerError::InvalidSocketTarget
    ));
    assert_eq!(
        fs::read(&socket_path)
            .unwrap_or_else(|error| panic!("sentinel must remain readable: {error}")),
        b"fictional sentinel"
    );
}

#[test]
fn listener_lock_prevents_a_second_owner() {
    let root = TestDirectory::create();
    let socket_path = root.path().join("queue-ingress.sock");
    let first = PublishedSocket::bind(&socket_path)
        .unwrap_or_else(|error| panic!("first listener must bind: {error}"));
    let error = match PublishedSocket::bind(&socket_path) {
        Ok(_) => panic!("second listener must not replace the live socket"),
        Err(error) => error,
    };
    assert!(matches!(error, QueueIngressListenerError::AlreadyRunning));
    drop(first);
    assert!(!socket_path.exists());
}

#[test]
fn publishes_a_socket_in_a_nested_runtime_directory() {
    let root = TestDirectory::create();
    let runtime = root.path().join("run");
    fs::create_dir(&runtime)
        .unwrap_or_else(|error| panic!("runtime directory must be created: {error}"));
    fs::set_permissions(&runtime, fs::Permissions::from_mode(0o2750))
        .unwrap_or_else(|error| panic!("runtime directory mode must be set: {error}"));
    let socket_path = runtime.join("queue-ingress.sock");

    let published = PublishedSocket::bind(&socket_path)
        .unwrap_or_else(|error| panic!("nested runtime socket must be published: {error}"));
    assert!(socket_path.exists());
    drop(published);
    assert!(!socket_path.exists());
}

#[test]
fn rejects_a_socket_parent_reached_through_a_symbolic_link() {
    let root = TestDirectory::create();
    let runtime = root.path().join("r");
    fs::create_dir(&runtime)
        .unwrap_or_else(|error| panic!("runtime directory must be created: {error}"));
    fs::set_permissions(&runtime, fs::Permissions::from_mode(0o2750))
        .unwrap_or_else(|error| panic!("runtime directory mode must be set: {error}"));
    let linked_runtime = root.path().join("l");
    symlink(&runtime, &linked_runtime)
        .unwrap_or_else(|error| panic!("runtime symlink must be created: {error}"));

    let error = match PublishedSocket::bind(&linked_runtime.join("s")) {
        Ok(_) => panic!("symbolic-link socket parent must be rejected"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        QueueIngressListenerError::NonCanonicalSocketParent
    ));
    assert!(!runtime.join(super::SOCKET_STATE_FILE).exists());
}

#[test]
fn rejects_a_runtime_path_without_room_for_atomic_publication() {
    let root = TestDirectory::create();
    let root_length = root.path().as_os_str().as_bytes().len();
    let padding_length = 77_usize.saturating_sub(root_length + 1).max(1);
    let runtime = root.path().join("r".repeat(padding_length));
    fs::create_dir(&runtime)
        .unwrap_or_else(|error| panic!("long runtime directory must be created: {error}"));
    fs::set_permissions(&runtime, fs::Permissions::from_mode(0o2750))
        .unwrap_or_else(|error| panic!("long runtime directory mode must be set: {error}"));
    let socket_path = runtime.join("s");
    UnixAddr::new(&socket_path)
        .unwrap_or_else(|error| panic!("configured public socket path must fit: {error}"));

    let error = match PublishedSocket::bind(&socket_path) {
        Ok(_) => panic!("runtime path without temporary-name space must be rejected"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        QueueIngressListenerError::TemporarySocketPath { .. }
    ));
    assert!(!root.path().join(super::SOCKET_STATE_FILE).exists());
}

#[test]
fn rejects_an_unaddressable_public_socket_path() {
    let root = TestDirectory::create();
    let root_length = root.path().as_os_str().as_bytes().len();
    let socket_name_length = 108_usize.saturating_sub(root_length + 1).max(1);
    let socket_path = root.path().join("s".repeat(socket_name_length));

    let error = match PublishedSocket::bind(&socket_path) {
        Ok(_) => panic!("unaddressable public socket path must be rejected"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        QueueIngressListenerError::SocketAddressPath { .. }
    ));
    assert!(!root.path().join(super::SOCKET_STATE_FILE).exists());
}

#[test]
fn refuses_to_replace_a_live_socket_without_the_listener_lock() {
    let root = TestDirectory::create();
    let socket_path = root.path().join("queue-ingress.sock");
    let live = UnixListener::bind(&socket_path)
        .unwrap_or_else(|error| panic!("external listener must bind: {error}"));

    let error = match PublishedSocket::bind(&socket_path) {
        Ok(_) => panic!("live socket must not be replaced"),
        Err(error) => error,
    };
    assert!(matches!(error, QueueIngressListenerError::AlreadyRunning));
    assert!(socket_path.exists());
    drop(live);
}

#[test]
fn socket_probe_remains_nonblocking_when_the_listener_backlog_is_full() {
    let root = TestDirectory::create();
    let socket_path = root.path().join("queue-ingress.sock");
    let address = UnixAddr::new(&socket_path)
        .unwrap_or_else(|error| panic!("backlog fixture address must be valid: {error}"));
    let listener = socket(
        AddressFamily::Unix,
        SockType::Stream,
        SockFlag::SOCK_CLOEXEC,
        None,
    )
    .unwrap_or_else(|error| panic!("backlog listener socket must open: {error}"));
    socket_bind(listener.as_raw_fd(), &address)
        .unwrap_or_else(|error| panic!("backlog fixture must bind: {error}"));
    socket_listen(
        &listener,
        Backlog::new(1).unwrap_or_else(|error| panic!("backlog must be valid: {error}")),
    )
    .unwrap_or_else(|error| panic!("backlog fixture must listen: {error}"));
    let mut clients = Vec::new();
    let mut saturated = false;
    for _ in 0..16 {
        let client = socket(
            AddressFamily::Unix,
            SockType::Stream,
            SockFlag::SOCK_CLOEXEC | SockFlag::SOCK_NONBLOCK,
            None,
        )
        .unwrap_or_else(|error| panic!("backlog client socket must open: {error}"));
        match connect(client.as_raw_fd(), &address) {
            Ok(()) | Err(Errno::EINPROGRESS | Errno::EALREADY) => clients.push(client),
            Err(Errno::EAGAIN) => {
                saturated = true;
                break;
            }
            Err(error) => panic!("backlog fill must have a predictable result: {error}"),
        }
    }
    assert!(saturated, "test must saturate the listener backlog");

    let started = Instant::now();
    assert_eq!(
        probe_socket(&socket_path)
            .unwrap_or_else(|error| panic!("saturated listener must be probed: {error}")),
        SocketProbe::Live
    );
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[test]
fn refuses_a_stale_socket_without_an_ownership_record() {
    let root = TestDirectory::create();
    let socket_path = root.path().join("queue-ingress.sock");
    let stale = UnixListener::bind(&socket_path)
        .unwrap_or_else(|error| panic!("stale listener fixture must bind: {error}"));
    drop(stale);

    let error = match PublishedSocket::bind(&socket_path) {
        Ok(_) => panic!("unowned stale socket must not be replaced"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        QueueIngressListenerError::UnownedStaleSocket
    ));
    assert!(socket_path.exists());
}

#[test]
fn replaces_a_stale_socket_recorded_by_a_prior_listener() {
    let root = TestDirectory::create();
    let socket_path = root.path().join("queue-ingress.sock");
    let stale = UnixListener::bind(&socket_path)
        .unwrap_or_else(|error| panic!("stale listener fixture must bind: {error}"));
    drop(stale);
    let metadata = fs::symlink_metadata(&socket_path)
        .unwrap_or_else(|error| panic!("stale socket must be inspectable: {error}"));
    write_socket_state(
        root.path(),
        &SocketState::Owned {
            public: b"queue-ingress.sock".to_vec(),
            identity: SocketIdentity::from_metadata(&metadata),
        },
    )
    .unwrap_or_else(|error| panic!("prior listener state must be recorded: {error}"));

    let published = PublishedSocket::bind(&socket_path)
        .unwrap_or_else(|error| panic!("owned stale socket must be replaced: {error}"));
    assert!(socket_path.exists());
    drop(published);
    assert!(!socket_path.exists());
}

#[test]
fn refuses_a_socket_name_change_while_the_prior_publication_exists() {
    let root = TestDirectory::create();
    let prior_socket_path = root.path().join("prior.sock");
    let stale = UnixListener::bind(&prior_socket_path)
        .unwrap_or_else(|error| panic!("prior listener fixture must bind: {error}"));
    drop(stale);
    let metadata = fs::symlink_metadata(&prior_socket_path)
        .unwrap_or_else(|error| panic!("prior socket must be inspectable: {error}"));
    write_socket_state(
        root.path(),
        &SocketState::Owned {
            public: b"prior.sock".to_vec(),
            identity: SocketIdentity::from_metadata(&metadata),
        },
    )
    .unwrap_or_else(|error| panic!("prior listener state must be recorded: {error}"));

    let replacement_socket_path = root.path().join("replacement.sock");
    let error = match PublishedSocket::bind(&replacement_socket_path) {
        Ok(_) => panic!("a socket name change must not orphan the prior publication"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        QueueIngressListenerError::SocketConfigurationChanged { .. }
    ));
    assert!(prior_socket_path.exists());
    assert!(!replacement_socket_path.exists());
}

#[test]
fn preparing_state_does_not_authorize_an_unrelated_public_socket() {
    let root = TestDirectory::create();
    let socket_path = root.path().join("queue-ingress.sock");
    let temporary = format!(
        "{}{}",
        super::TEMPORARY_SOCKET_PREFIX,
        ulid::Ulid::generate()
    );
    write_socket_state(
        root.path(),
        &SocketState::Preparing {
            public: b"queue-ingress.sock".to_vec(),
            temporary,
        },
    )
    .unwrap_or_else(|error| panic!("preparing state must be durable: {error}"));
    let stale = UnixListener::bind(&socket_path)
        .unwrap_or_else(|error| panic!("unrelated stale listener must bind: {error}"));
    drop(stale);

    let error = match PublishedSocket::bind(&socket_path) {
        Ok(_) => panic!("preparing state must not authorize the public socket"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        QueueIngressListenerError::UnownedStaleSocket
    ));
    assert!(socket_path.exists());
}

#[test]
fn legacy_preparing_state_is_read_without_claiming_the_public_socket() {
    let empty = TestDirectory::create();
    let legacy_state = format!("{} preparing\n", super::LEGACY_SOCKET_STATE_FORMAT);
    let empty_state_path = empty.path().join(super::SOCKET_STATE_FILE);
    fs::write(&empty_state_path, &legacy_state)
        .unwrap_or_else(|error| panic!("legacy state must be written: {error}"));
    fs::set_permissions(&empty_state_path, fs::Permissions::from_mode(0o600))
        .unwrap_or_else(|error| panic!("legacy state mode must be set: {error}"));
    let empty_socket_path = empty.path().join("queue-ingress.sock");
    let published = PublishedSocket::bind(&empty_socket_path)
        .unwrap_or_else(|error| panic!("legacy state without a socket must recover: {error}"));
    drop(published);

    let occupied = TestDirectory::create();
    let occupied_state_path = occupied.path().join(super::SOCKET_STATE_FILE);
    fs::write(&occupied_state_path, legacy_state)
        .unwrap_or_else(|error| panic!("legacy state must be written: {error}"));
    fs::set_permissions(&occupied_state_path, fs::Permissions::from_mode(0o600))
        .unwrap_or_else(|error| panic!("legacy state mode must be set: {error}"));
    let occupied_socket_path = occupied.path().join("queue-ingress.sock");
    let stale = UnixListener::bind(&occupied_socket_path)
        .unwrap_or_else(|error| panic!("unrelated stale socket must bind: {error}"));
    drop(stale);

    let error = match PublishedSocket::bind(&occupied_socket_path) {
        Ok(_) => panic!("legacy preparing state must not claim a public socket"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        QueueIngressListenerError::UnownedStaleSocket
    ));
}

#[test]
fn legacy_owned_state_recovers_the_configured_public_socket() {
    let root = TestDirectory::create();
    let socket_path = root.path().join("queue-ingress.sock");
    let stale = UnixListener::bind(&socket_path)
        .unwrap_or_else(|error| panic!("legacy stale listener fixture must bind: {error}"));
    drop(stale);
    let metadata = fs::symlink_metadata(&socket_path)
        .unwrap_or_else(|error| panic!("legacy stale socket must be inspectable: {error}"));
    let legacy_state = format!(
        "{} owned {} {}\n",
        super::LEGACY_SOCKET_STATE_FORMAT,
        metadata.dev(),
        metadata.ino()
    );
    let state_path = root.path().join(super::SOCKET_STATE_FILE);
    fs::write(&state_path, legacy_state)
        .unwrap_or_else(|error| panic!("legacy owned state must be written: {error}"));
    fs::set_permissions(&state_path, fs::Permissions::from_mode(0o600))
        .unwrap_or_else(|error| panic!("legacy state mode must be set: {error}"));

    let published = PublishedSocket::bind(&socket_path)
        .unwrap_or_else(|error| panic!("legacy owned socket must be recovered: {error}"));
    drop(published);
    assert!(!socket_path.exists());
}

#[test]
fn preparing_state_recovers_its_unique_temporary_socket() {
    let root = TestDirectory::create();
    let temporary = format!(
        "{}{}",
        super::TEMPORARY_SOCKET_PREFIX,
        ulid::Ulid::generate()
    );
    let temporary_path = root.path().join(&temporary);
    let stale = UnixListener::bind(&temporary_path)
        .unwrap_or_else(|error| panic!("temporary socket fixture must bind: {error}"));
    drop(stale);
    write_socket_state(
        root.path(),
        &SocketState::Preparing {
            public: b"queue-ingress.sock".to_vec(),
            temporary,
        },
    )
    .unwrap_or_else(|error| panic!("preparing state must be durable: {error}"));

    let socket_path = root.path().join("queue-ingress.sock");
    let published = PublishedSocket::bind(&socket_path)
        .unwrap_or_else(|error| panic!("recorded temporary socket must be recovered: {error}"));
    assert!(!temporary_path.exists());
    drop(published);
}

#[test]
fn prepared_state_recovers_a_socket_renamed_before_final_state() {
    let root = TestDirectory::create();
    let socket_path = root.path().join("queue-ingress.sock");
    let stale = UnixListener::bind(&socket_path)
        .unwrap_or_else(|error| panic!("renamed socket fixture must bind: {error}"));
    drop(stale);
    let metadata = fs::symlink_metadata(&socket_path)
        .unwrap_or_else(|error| panic!("renamed socket must be inspectable: {error}"));
    let temporary = format!(
        "{}{}",
        super::TEMPORARY_SOCKET_PREFIX,
        ulid::Ulid::generate()
    );
    write_socket_state(
        root.path(),
        &SocketState::Prepared {
            public: b"queue-ingress.sock".to_vec(),
            temporary,
            identity: SocketIdentity::from_metadata(&metadata),
        },
    )
    .unwrap_or_else(|error| panic!("prepared state must be durable: {error}"));
    fs::write(
        root.path().join(super::SOCKET_STATE_TEMPORARY_FILE),
        b"fictional interrupted replacement",
    )
    .unwrap_or_else(|error| panic!("interrupted temporary state must be written: {error}"));

    let published = PublishedSocket::bind(&socket_path)
        .unwrap_or_else(|error| panic!("prepared state must authorize recovery: {error}"));
    drop(published);
    assert!(!socket_path.exists());
}

#[test]
fn refuses_a_runtime_directory_in_an_unprotected_namespace() {
    let root = TestDirectory::create();
    let namespace = root.path().join("n");
    let protected_child = namespace.join("p");
    let runtime = protected_child.join("r");
    fs::create_dir(&namespace)
        .unwrap_or_else(|error| panic!("namespace directory must be created: {error}"));
    fs::set_permissions(&namespace, fs::Permissions::from_mode(0o0770))
        .unwrap_or_else(|error| panic!("unsafe namespace mode must be set: {error}"));
    fs::create_dir(&protected_child)
        .unwrap_or_else(|error| panic!("protected child must be created: {error}"));
    fs::set_permissions(&protected_child, fs::Permissions::from_mode(0o0750))
        .unwrap_or_else(|error| panic!("protected child mode must be set: {error}"));
    fs::create_dir(&runtime)
        .unwrap_or_else(|error| panic!("runtime directory must be created: {error}"));
    fs::set_permissions(&runtime, fs::Permissions::from_mode(0o2750))
        .unwrap_or_else(|error| panic!("runtime directory mode must be set: {error}"));

    let error = match PublishedSocket::bind(&runtime.join("s")) {
        Ok(_) => panic!("an unprotected namespace must be rejected"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        QueueIngressListenerError::UnsafeSocketNamespace
    ));
}

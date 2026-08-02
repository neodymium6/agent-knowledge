use std::fs;
use std::io::Cursor;
use std::num::NonZeroUsize;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use agent_knowledge_gateway::{IngressClient, IngressClientError};
use agent_knowledge_protocol::{ClientId, StatusRequest};
use agent_knowledge_queue::{FileQueue, PackagePolicy};
use tar::{Builder, EntryType, Header};

use super::{
    ListenSettings, PublishedSocket, QueueIngressListenerError, SOCKET_MODE, SocketIdentity,
    SocketState, listen_until, write_socket_state,
};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

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
    let listener = thread::spawn(move || {
        let mut output = Vec::new();
        let result = listen_until(settings, &mut output, || {
            listener_stopping.load(Ordering::Relaxed)
        });
        (result, output)
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
        SocketState::Owned(SocketIdentity::from_metadata(&metadata)),
    )
    .unwrap_or_else(|error| panic!("prior listener state must be recorded: {error}"));

    let published = PublishedSocket::bind(&socket_path)
        .unwrap_or_else(|error| panic!("owned stale socket must be replaced: {error}"));
    assert!(socket_path.exists());
    drop(published);
    assert!(!socket_path.exists());
}

#[test]
fn recovers_a_socket_left_after_preparing_state_was_published() {
    let root = TestDirectory::create();
    let socket_path = root.path().join("queue-ingress.sock");
    write_socket_state(root.path(), SocketState::Preparing)
        .unwrap_or_else(|error| panic!("preparing state must be durable: {error}"));
    fs::write(
        root.path().join(super::SOCKET_STATE_TEMPORARY_FILE),
        b"fictional interrupted replacement",
    )
    .unwrap_or_else(|error| panic!("interrupted temporary state must be written: {error}"));
    let stale = UnixListener::bind(&socket_path)
        .unwrap_or_else(|error| panic!("interrupted listener fixture must bind: {error}"));
    drop(stale);

    let published = PublishedSocket::bind(&socket_path)
        .unwrap_or_else(|error| panic!("preparing state must authorize recovery: {error}"));
    drop(published);
    assert!(!socket_path.exists());
}

#[test]
fn refuses_a_runtime_directory_in_an_unprotected_namespace() {
    let root = TestDirectory::create();
    let namespace = root.path().join("namespace");
    let runtime = namespace.join("run");
    fs::create_dir(&namespace)
        .unwrap_or_else(|error| panic!("namespace directory must be created: {error}"));
    fs::set_permissions(&namespace, fs::Permissions::from_mode(0o0770))
        .unwrap_or_else(|error| panic!("unsafe namespace mode must be set: {error}"));
    fs::create_dir(&runtime)
        .unwrap_or_else(|error| panic!("runtime directory must be created: {error}"));
    fs::set_permissions(&runtime, fs::Permissions::from_mode(0o2750))
        .unwrap_or_else(|error| panic!("runtime directory mode must be set: {error}"));

    let error = match PublishedSocket::bind(&runtime.join("queue-ingress.sock")) {
        Ok(_) => panic!("an unprotected namespace must be rejected"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        QueueIngressListenerError::UnsafeSocketNamespace
    ));
}

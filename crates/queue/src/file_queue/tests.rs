use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use agent_knowledge_core::{BatchId, ErrorCode, PayloadPath, RequestId};
use agent_knowledge_protocol::ClientId;

use super::status::StatusObservationHook;
use super::{
    AcceptanceHook, AcceptancePhase, ClaimToken, DirectoryScanner, EnqueueOutcome, FileQueue,
    IncomingPackage, ProcessingScanOutcome, QueueError, QueueLimit, QueueReader,
    QueueRequestStatus, QueueState, StaleAgeSource, WorkerQueueError, inactive_stale_directories,
};
use crate::{PackageLimits, PackagePolicy, validate_accepted_package};

const REQUEST_JSON: &str = r#"{
    "protocol_version": 1,
    "request_id": "01K00000000000000000000000",
    "title": "Record fictional benchmark",
    "project": "fictional-solver",
    "document_type": "experiment",
    "node": "fictional-node-a",
    "agent": "codex",
    "session": "01K00000000000000000000001",
    "created_at": "2026-07-31T03:50:00+09:00",
    "operations": [
        {
            "type": "create_document",
            "document_id": "01K00000000000000000000002",
            "content": "benchmark/index.md"
        },
        {
            "type": "add_attachment",
            "document_id": "01K00000000000000000000002",
            "source": "benchmark/results.csv",
            "name": "results.csv"
        }
    ]
}"#;

const MARKDOWN: &[u8] = b"---\n\
schema_version: 1\n\
document_id: 01K00000000000000000000002\n\
title: Fictional benchmark\n\
created: 2026-07-31T03:50:00+09:00\n\
request_id: 01K00000000000000000000000\n\
tags:\n\
  - benchmark\n\
status: active\n\
---\n";
const RESULTS: &[u8] = b"step,value\n1,42\n";

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn create() -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "agent-knowledge-file-queue-test-{}-{sequence}",
            std::process::id()
        ));
        if let Err(error) = fs::create_dir(&path) {
            panic!(
                "test directory must be created at {}: {error}",
                path.display()
            );
        }
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.0)
            && error.kind() != io::ErrorKind::NotFound
        {
            panic!(
                "test directory must be removed at {}: {error}",
                self.0.display()
            );
        }
    }
}

fn initialize_queue(root: &Path, policy: PackagePolicy) -> FileQueue {
    match FileQueue::initialize(root.join("queue"), policy) {
        Ok(queue) => queue,
        Err(error) => panic!("fixture queue must initialize: {error}"),
    }
}

fn payload_path(value: &str) -> PayloadPath {
    match value.parse() {
        Ok(path) => path,
        Err(error) => panic!("fixture payload path must parse: {error}"),
    }
}

fn stage_package(queue: &FileQueue, results: &[u8]) -> IncomingPackage {
    stage_package_with_request_id(queue, results, "01K00000000000000000000000")
}

fn stage_package_with_request_id(
    queue: &FileQueue,
    results: &[u8],
    request_id: &str,
) -> IncomingPackage {
    let mut package = match queue.begin() {
        Ok(package) => package,
        Err(error) => panic!("incoming package must begin: {error}"),
    };
    let request = REQUEST_JSON.replace("01K00000000000000000000000", request_id);
    if let Err(error) = package.write_request(request.as_bytes()) {
        panic!("fixture request must be written: {error}");
    }
    let markdown =
        String::from_utf8_lossy(MARKDOWN).replace("01K00000000000000000000000", request_id);
    if let Err(error) = package.add_payload(payload_path("benchmark/index.md"), markdown.as_bytes())
    {
        panic!("fixture Markdown must be written: {error}");
    }
    if let Err(error) = package.add_payload(payload_path("benchmark/results.csv"), results) {
        panic!("fixture attachment must be written: {error}");
    }
    package
}

fn accept(package: IncomingPackage) -> EnqueueOutcome {
    match package.accept() {
        Ok(outcome) => outcome,
        Err(error) => panic!("fixture package must be accepted: {error}"),
    }
}

fn incoming_is_empty(root: &Path) -> bool {
    let mut entries = match fs::read_dir(root.join("queue/incoming")) {
        Ok(entries) => entries,
        Err(error) => panic!("incoming directory must be readable: {error}"),
    };
    entries.next().is_none()
}

fn copy_tree(source: &Path, destination: &Path) {
    fs::create_dir(destination)
        .unwrap_or_else(|error| panic!("copied directory must be created: {error}"));
    let entries = fs::read_dir(source)
        .unwrap_or_else(|error| panic!("source directory must be readable: {error}"));
    for entry in entries {
        let entry = entry.unwrap_or_else(|error| panic!("source entry must be readable: {error}"));
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let metadata = entry
            .metadata()
            .unwrap_or_else(|error| panic!("source metadata must be readable: {error}"));
        if metadata.is_dir() {
            copy_tree(&source_path, &destination_path);
        } else if metadata.is_file() {
            fs::copy(&source_path, &destination_path)
                .unwrap_or_else(|error| panic!("source file must be copied: {error}"));
        } else {
            panic!("queue fixture must contain only directories and regular files");
        }
    }
}

fn request_id(value: &str) -> RequestId {
    value
        .parse()
        .unwrap_or_else(|error| panic!("fixture request ID must parse: {error}"))
}

fn batch_id(value: &str) -> BatchId {
    value
        .parse()
        .unwrap_or_else(|error| panic!("fixture batch ID must parse: {error}"))
}

#[test]
fn read_only_status_observes_every_request_state_without_taking_queue_locks() {
    const SECOND_REQUEST_ID: &str = "01K00000000000000000000010";
    const FIRST_BATCH_ID: &str = "01K00000000000000000000020";
    const SECOND_BATCH_ID: &str = "01K00000000000000000000021";

    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    accept(stage_package(&queue, RESULTS));
    let first_request = request_id("01K00000000000000000000000");

    let lock = queue
        .open_queue_lock()
        .unwrap_or_else(|error| panic!("queue lock fixture must open: {error}"));
    lock.lock()
        .unwrap_or_else(|error| panic!("queue lock fixture must lock: {error}"));
    let reader = QueueReader::open_until(root.path().join("queue"), None).unwrap_or_else(|error| {
        panic!("read-only queue must open without taking the lock: {error}")
    });
    assert_eq!(
        reader
            .status_until(first_request, None)
            .unwrap_or_else(|error| panic!("pending status must be readable: {error}")),
        Some(QueueRequestStatus::Pending)
    );
    drop(lock);

    let mut worker = queue
        .try_worker_session()
        .unwrap_or_else(|error| panic!("Worker fixture must open: {error}"));
    assert!(matches!(
        worker.scan_processing(16),
        Ok(ProcessingScanOutcome::Complete { ref claims, .. }) if claims.is_empty()
    ));
    let completed = worker
        .claim(first_request, batch_id(FIRST_BATCH_ID))
        .unwrap_or_else(|error| panic!("first request must be claimed: {error}"))
        .token();
    assert_eq!(
        reader
            .status_until(first_request, None)
            .unwrap_or_else(|error| panic!("processing status must be readable: {error}")),
        Some(QueueRequestStatus::Processing)
    );
    worker
        .reconcile_batch(batch_id(FIRST_BATCH_ID), &[completed], &[])
        .unwrap_or_else(|error| panic!("first request must complete: {error}"));
    assert_eq!(
        reader
            .status_until(first_request, None)
            .unwrap_or_else(|error| panic!("completed status must be readable: {error}")),
        Some(QueueRequestStatus::Completed)
    );

    accept(stage_package_with_request_id(
        &queue,
        RESULTS,
        SECOND_REQUEST_ID,
    ));
    let second_request = request_id(SECOND_REQUEST_ID);
    let failed = worker
        .claim(second_request, batch_id(SECOND_BATCH_ID))
        .unwrap_or_else(|error| panic!("second request must be claimed: {error}"))
        .token();
    worker
        .reconcile_batch(
            batch_id(SECOND_BATCH_ID),
            &[],
            &[(failed, ErrorCode::RevisionConflict)],
        )
        .unwrap_or_else(|error| panic!("second request must fail: {error}"));
    assert!(matches!(
        reader
            .status_until(second_request, None)
            .unwrap_or_else(|error| panic!("failed status must be readable: {error}")),
        Some(QueueRequestStatus::Failed {
            error_code: ErrorCode::RevisionConflict,
            ..
        })
    ));
    assert_eq!(
        reader
            .status_until(request_id("01K00000000000000000000099"), None)
            .unwrap_or_else(|error| panic!("missing status lookup must succeed: {error}")),
        None
    );
}

#[test]
fn operational_overview_counts_states_and_observes_the_worker_lock() {
    const SECOND_REQUEST_ID: &str = "01K00000000000000000000010";
    const THIRD_REQUEST_ID: &str = "01K00000000000000000000011";
    const FIRST_BATCH_ID: &str = "01K00000000000000000000020";
    const SECOND_BATCH_ID: &str = "01K00000000000000000000021";

    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    accept(stage_package(&queue, RESULTS));
    accept(stage_package_with_request_id(
        &queue,
        RESULTS,
        SECOND_REQUEST_ID,
    ));
    accept(stage_package_with_request_id(
        &queue,
        RESULTS,
        THIRD_REQUEST_ID,
    ));
    let mut worker = queue
        .try_worker_session()
        .unwrap_or_else(|error| panic!("Worker fixture must open: {error}"));
    assert!(matches!(
        worker.scan_processing(16),
        Ok(ProcessingScanOutcome::Complete { ref claims, .. }) if claims.is_empty()
    ));
    let completed = worker
        .claim(
            request_id("01K00000000000000000000000"),
            batch_id(FIRST_BATCH_ID),
        )
        .unwrap_or_else(|error| panic!("first request must be claimed: {error}"))
        .token();
    worker
        .reconcile_batch(batch_id(FIRST_BATCH_ID), &[completed], &[])
        .unwrap_or_else(|error| panic!("first request must complete: {error}"));
    let failed = worker
        .claim(request_id(SECOND_REQUEST_ID), batch_id(SECOND_BATCH_ID))
        .unwrap_or_else(|error| panic!("second request must be claimed: {error}"))
        .token();
    worker
        .reconcile_batch(
            batch_id(SECOND_BATCH_ID),
            &[],
            &[(failed, ErrorCode::RevisionConflict)],
        )
        .unwrap_or_else(|error| panic!("second request must fail: {error}"));

    let reader = QueueReader::open_until(root.path().join("queue"), None)
        .unwrap_or_else(|error| panic!("read-only queue fixture must open: {error}"));
    let overview = reader
        .overview_until(3, None)
        .unwrap_or_else(|error| panic!("bounded overview must succeed: {error}"));
    assert_eq!(overview.pending(), 1);
    assert_eq!(overview.processing(), 0);
    assert_eq!(overview.completed(), 1);
    assert_eq!(overview.failed(), 1);
    assert!(overview.oldest_pending_at().is_some());
    assert!(overview.worker_active());
    assert!(!overview.counts_exact());
    assert!(matches!(
        reader.overview_until(2, None),
        Err(QueueError::StatusScanLimitExceeded { maximum: 2 })
    ));

    drop(worker);
    let overview = reader
        .overview_until(3, None)
        .unwrap_or_else(|error| panic!("unlocked overview must succeed: {error}"));
    assert!(!overview.worker_active());
}

#[test]
fn operational_overview_rejects_invalid_bounds_and_pending_metadata() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    accept(stage_package(&queue, RESULTS));
    let reader = QueueReader::open_until(root.path().join("queue"), None)
        .unwrap_or_else(|error| panic!("read-only queue fixture must open: {error}"));

    assert!(matches!(
        reader.overview_until(0, None),
        Err(QueueError::InvalidStatusScanLimit)
    ));
    let mutation = queue
        .open_queue_lock()
        .unwrap_or_else(|error| panic!("queue lock fixture must open: {error}"));
    mutation
        .lock()
        .unwrap_or_else(|error| panic!("queue mutation fixture must lock: {error}"));
    let overview = reader
        .overview_until(1, None)
        .unwrap_or_else(|error| panic!("overview must not wait for the mutation lock: {error}"));
    assert_eq!(overview.pending(), 1);
    drop(mutation);
    fs::write(
        root.path()
            .join("queue/pending/01K00000000000000000000000/acceptance.json"),
        b"{not-json}\n",
    )
    .unwrap_or_else(|error| panic!("acceptance fixture must be corrupted: {error}"));
    assert!(matches!(
        reader.overview_until(1, None),
        Err(QueueError::CorruptState {
            state: QueueState::Pending,
            detail: "pending request acceptance metadata is invalid",
            ..
        })
    ));
}

#[test]
fn operational_overview_deduplicates_a_request_observed_in_multiple_states() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    accept(stage_package(&queue, RESULTS));
    let duplicated = root
        .path()
        .join("queue/completed/01K00000000000000000000000");
    fs::create_dir(&duplicated)
        .unwrap_or_else(|error| panic!("duplicate state fixture must be created: {error}"));
    let reader = QueueReader::open_until(root.path().join("queue"), None)
        .unwrap_or_else(|error| panic!("read-only queue fixture must open: {error}"));

    let overview = reader
        .overview_until(2, None)
        .unwrap_or_else(|error| panic!("best-effort overview must deduplicate: {error}"));
    assert_eq!(overview.pending(), 1);
    assert_eq!(overview.completed(), 0);
    assert!(!overview.counts_exact());
}

#[cfg(unix)]
#[test]
fn operational_overview_rejects_a_fifo_acceptance_file_without_blocking() {
    use std::process::Command;

    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    accept(stage_package(&queue, RESULTS));
    let acceptance = root
        .path()
        .join("queue/pending/01K00000000000000000000000/acceptance.json");
    fs::remove_file(&acceptance)
        .unwrap_or_else(|error| panic!("acceptance fixture must be removed: {error}"));
    let created = Command::new("mkfifo")
        .arg(&acceptance)
        .status()
        .unwrap_or_else(|error| panic!("mkfifo fixture command must run: {error}"));
    assert!(created.success(), "FIFO fixture must be created");
    let reader = QueueReader::open_until(root.path().join("queue"), None)
        .unwrap_or_else(|error| panic!("read-only queue fixture must open: {error}"));

    assert!(matches!(
        reader.overview_until(1, None),
        Err(QueueError::CorruptState {
            state: QueueState::Pending,
            detail: "pending request acceptance metadata is invalid",
            ..
        })
    ));
}

#[cfg(unix)]
#[test]
fn operational_overview_rejects_a_hard_linked_acceptance_file() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    accept(stage_package(&queue, RESULTS));
    let acceptance = root
        .path()
        .join("queue/pending/01K00000000000000000000000/acceptance.json");
    fs::hard_link(&acceptance, root.path().join("linked-acceptance.json"))
        .unwrap_or_else(|error| panic!("hard-link fixture must be created: {error}"));
    let reader = QueueReader::open_until(root.path().join("queue"), None)
        .unwrap_or_else(|error| panic!("read-only queue fixture must open: {error}"));

    assert!(matches!(
        reader.overview_until(1, None),
        Err(QueueError::CorruptState {
            state: QueueState::Pending,
            detail: "pending request acceptance metadata is invalid",
            ..
        })
    ));
}

#[test]
fn read_only_status_requires_an_existing_queue_and_honors_deadlines() {
    let root = TestDirectory::create();
    let queue_root = root.path().join("queue");
    assert!(matches!(
        QueueReader::open_until(&queue_root, None),
        Err(QueueError::Io(error)) if error.kind() == io::ErrorKind::NotFound
    ));
    assert!(!queue_root.exists());

    let _queue = initialize_queue(root.path(), PackagePolicy::default());
    assert!(matches!(
        QueueReader::open_until(&queue_root, Some(std::time::Instant::now())),
        Err(QueueError::OperationDeadlineExceeded)
    ));

    let reader = QueueReader::open_until(&queue_root, None)
        .unwrap_or_else(|error| panic!("read-only queue fixture must open: {error}"));
    assert!(matches!(
        reader.status_until(
            request_id("01K00000000000000000000099"),
            Some(std::time::Instant::now())
        ),
        Err(QueueError::OperationDeadlineExceeded)
    ));
}

#[test]
fn read_only_status_rejects_queue_replacement() {
    let root = TestDirectory::create();
    let queue_root = root.path().join("queue");
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    accept(stage_package(&queue, RESULTS));
    let reader = QueueReader::open_until(&queue_root, None)
        .unwrap_or_else(|error| panic!("read-only queue fixture must open: {error}"));

    fs::rename(&queue_root, root.path().join("detached-queue"))
        .unwrap_or_else(|error| panic!("queue fixture must be detached: {error}"));
    FileQueue::initialize(&queue_root, PackagePolicy::default())
        .unwrap_or_else(|error| panic!("replacement queue fixture must initialize: {error}"));
    assert!(matches!(
        reader.status_until(request_id("01K00000000000000000000000"), None),
        Err(QueueError::InvalidQueueIdentity)
    ));
}

#[test]
fn read_only_status_retries_an_empty_observation_during_requeue() {
    const BATCH_ID: &str = "01K00000000000000000000020";

    struct RequeueAfterPending<'a> {
        worker: &'a mut super::WorkerSession,
        token: Option<ClaimToken>,
    }

    impl StatusObservationHook for RequeueAfterPending<'_> {
        fn after_state(&mut self, state: QueueState) {
            if state == QueueState::Pending
                && let Some(token) = self.token.take()
            {
                self.worker.requeue_claimed(token).unwrap_or_else(|error| {
                    panic!("processing request must be requeued during observation: {error}")
                });
            }
        }
    }

    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    accept(stage_package(&queue, RESULTS));
    let request_id = request_id("01K00000000000000000000000");
    let mut worker = queue
        .try_worker_session()
        .unwrap_or_else(|error| panic!("Worker fixture must open: {error}"));
    assert!(matches!(
        worker.scan_processing(16),
        Ok(ProcessingScanOutcome::Complete { ref claims, .. }) if claims.is_empty()
    ));
    let token = worker
        .claim(request_id, batch_id(BATCH_ID))
        .unwrap_or_else(|error| panic!("request must be claimed: {error}"))
        .token();
    let reader = QueueReader::open_until(root.path().join("queue"), None)
        .unwrap_or_else(|error| panic!("read-only queue fixture must open: {error}"));
    let mut hook = RequeueAfterPending {
        worker: &mut worker,
        token: Some(token),
    };

    assert_eq!(
        reader
            .status_until_with_hook(request_id, None, &mut hook)
            .unwrap_or_else(|error| panic!("requeued status must be retried: {error}")),
        Some(QueueRequestStatus::Pending)
    );
    assert!(hook.token.is_none());
}

#[test]
fn initialization_requires_an_existing_parent_directory() {
    let root = TestDirectory::create();
    let queue_root = root.path().join("missing-parent").join("queue");
    assert!(matches!(
        FileQueue::initialize(&queue_root, PackagePolicy::default()),
        Err(QueueError::Io(error)) if error.kind() == io::ErrorKind::NotFound
    ));
    assert!(!queue_root.exists());
}

#[test]
fn a_replaced_queue_invalidates_gateway_staging_and_acceptance() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    let staged = stage_package(&queue, RESULTS);
    let queue_path = root.path().join("queue");
    let detached_path = root.path().join("detached-queue");
    if let Err(error) = fs::rename(&queue_path, &detached_path) {
        panic!("original queue must be moved aside: {error}");
    }
    let replacement = match FileQueue::initialize(&queue_path, PackagePolicy::default()) {
        Ok(queue) => queue,
        Err(error) => panic!("replacement queue must initialize: {error}"),
    };
    fs::copy(detached_path.join("queue-id"), queue_path.join("queue-id"))
        .unwrap_or_else(|error| panic!("replacement must preserve the copied queue ID: {error}"));

    assert!(matches!(
        queue.begin(),
        Err(QueueError::InvalidQueueIdentity)
    ));
    assert!(matches!(
        staged.accept(),
        Err(QueueError::InvalidQueueIdentity)
    ));
    assert!(incoming_is_empty(root.path()));
    assert!(
        fs::read_dir(detached_path.join("pending"))
            .map(|mut entries| entries.next().is_none())
            .unwrap_or(false)
    );
    drop(replacement);
}

#[test]
fn rejects_a_copied_queue_as_a_second_live_instance() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    accept(stage_package(&queue, RESULTS));
    let copied = root.path().join("copied-queue");
    copy_tree(&root.path().join("queue"), &copied);

    assert!(matches!(
        FileQueue::initialize(&copied, PackagePolicy::default()),
        Err(QueueError::InvalidQueueIdentity)
    ));
}

#[test]
fn rejects_replaced_fixed_lock_files() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    let queue_lock = root.path().join("queue/.locks/queue.lock");
    fs::rename(&queue_lock, queue_lock.with_extension("detached"))
        .unwrap_or_else(|error| panic!("queue lock must be moved aside: {error}"));
    fs::write(&queue_lock, [])
        .unwrap_or_else(|error| panic!("replacement queue lock must be created: {error}"));

    assert!(matches!(
        queue.begin(),
        Err(QueueError::InvalidQueueIdentity)
    ));
    assert!(matches!(
        FileQueue::initialize(root.path().join("queue"), PackagePolicy::default()),
        Err(QueueError::InvalidQueueIdentity)
    ));

    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    let worker = queue
        .try_worker_session()
        .unwrap_or_else(|error| panic!("original Worker lock must be held: {error}"));
    let worker_lock = root.path().join("queue/.locks/repository-writer.lock");
    fs::rename(&worker_lock, worker_lock.with_extension("detached"))
        .unwrap_or_else(|error| panic!("Worker lock must be moved aside: {error}"));
    fs::write(&worker_lock, [])
        .unwrap_or_else(|error| panic!("replacement Worker lock must be created: {error}"));

    assert!(matches!(
        worker.queue_identity(),
        Err(WorkerQueueError::Queue(QueueError::InvalidQueueIdentity))
    ));
    assert!(matches!(
        FileQueue::initialize(root.path().join("queue"), PackagePolicy::default()),
        Err(QueueError::InvalidQueueIdentity)
    ));
    drop(worker);
}

#[test]
fn rejects_replaced_fixed_queue_directories() {
    for directory in [
        ".locks",
        "incoming",
        "quarantine",
        "worker-tmp",
        "pending",
        "processing",
        "completed",
        "failed",
    ] {
        let root = TestDirectory::create();
        let queue = initialize_queue(root.path(), PackagePolicy::default());
        let entry = root.path().join("queue").join(directory);
        let detached = root.path().join(format!("detached-{directory}"));
        fs::rename(&entry, &detached)
            .unwrap_or_else(|error| panic!("{directory} must be moved aside: {error}"));
        fs::create_dir(&entry)
            .unwrap_or_else(|error| panic!("replacement {directory} must be created: {error}"));

        assert!(matches!(
            queue.begin(),
            Err(QueueError::InvalidQueueIdentity)
        ));
        assert!(matches!(
            FileQueue::initialize(root.path().join("queue"), PackagePolicy::default()),
            Err(QueueError::InvalidQueueIdentity)
        ));
    }
}

#[cfg(unix)]
#[test]
fn rejects_symlinks_that_restore_replaced_queue_entries() {
    use std::os::unix::fs::symlink;

    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    let queue_entry = root.path().join("queue");
    let detached_queue = root.path().join("detached-queue");
    fs::rename(&queue_entry, &detached_queue)
        .unwrap_or_else(|error| panic!("queue root must be moved aside: {error}"));
    symlink(&detached_queue, &queue_entry)
        .unwrap_or_else(|error| panic!("queue root symlink must be created: {error}"));
    assert!(matches!(
        queue.begin(),
        Err(QueueError::InvalidQueueIdentity)
    ));

    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    let pending = root.path().join("queue/pending");
    let detached_pending = root.path().join("detached-pending");
    fs::rename(&pending, &detached_pending)
        .unwrap_or_else(|error| panic!("pending directory must be moved aside: {error}"));
    symlink(&detached_pending, &pending)
        .unwrap_or_else(|error| panic!("pending symlink must be created: {error}"));
    assert!(matches!(
        queue.begin(),
        Err(QueueError::InvalidQueueIdentity)
    ));

    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    let queue_lock = root.path().join("queue/.locks/queue.lock");
    let detached_lock = root.path().join("detached-queue-lock");
    fs::rename(&queue_lock, &detached_lock)
        .unwrap_or_else(|error| panic!("queue lock must be moved aside: {error}"));
    symlink(&detached_lock, &queue_lock)
        .unwrap_or_else(|error| panic!("queue lock symlink must be created: {error}"));
    assert!(matches!(
        queue.begin(),
        Err(QueueError::InvalidQueueIdentity)
    ));
}

#[cfg(unix)]
#[test]
fn rejects_an_ancestor_symlink_that_restores_the_queue_root() {
    use std::os::unix::fs::symlink;

    let root = TestDirectory::create();
    let ancestor = root.path().join("configured");
    fs::create_dir(&ancestor)
        .unwrap_or_else(|error| panic!("configured ancestor must be created: {error}"));
    let queue = FileQueue::initialize(ancestor.join("queue"), PackagePolicy::default())
        .unwrap_or_else(|error| panic!("fixture queue must initialize: {error}"));
    let detached_ancestor = root.path().join("detached-configured");
    fs::rename(&ancestor, &detached_ancestor)
        .unwrap_or_else(|error| panic!("configured ancestor must be moved aside: {error}"));
    symlink(&detached_ancestor, &ancestor)
        .unwrap_or_else(|error| panic!("ancestor symlink must be created: {error}"));

    assert!(matches!(
        queue.begin(),
        Err(QueueError::InvalidQueueIdentity)
    ));
}

#[test]
fn accepts_new_package_and_returns_existing_for_an_identical_retry() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());

    let accepted = accept(stage_package(&queue, RESULTS));
    let (request_id, digest) = match accepted {
        EnqueueOutcome::Accepted { request_id, digest } => (request_id, digest),
        EnqueueOutcome::Existing { .. } => panic!("first request must be newly accepted"),
    };

    let pending = root
        .path()
        .join("queue/pending")
        .join(request_id.to_string());
    assert!(pending.join("request.json").is_file());
    assert!(pending.join("payload/benchmark/index.md").is_file());
    let stored_digest = match fs::read_to_string(pending.join("digest")) {
        Ok(stored_digest) => stored_digest,
        Err(error) => panic!("stored digest must be readable: {error}"),
    };
    assert_eq!(stored_digest, format!("{digest}\n"));
    let validated = match validate_accepted_package(&pending, &PackagePolicy::default()) {
        Ok(validated) => validated,
        Err(error) => panic!("accepted package must revalidate: {error}"),
    };
    assert_eq!(
        validated
            .acceptance()
            .map(|metadata| metadata.sequence.get()),
        Some(1)
    );
    assert!(incoming_is_empty(root.path()));

    assert_eq!(
        accept(stage_package(&queue, RESULTS)),
        EnqueueOutcome::Existing {
            request_id,
            digest,
            state: QueueState::Pending,
        }
    );
    assert!(incoming_is_empty(root.path()));
}

#[test]
fn authenticated_acceptance_preserves_the_original_client_identity() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    let first_client = "fictional-node-a"
        .parse::<ClientId>()
        .unwrap_or_else(|error| panic!("first client ID must parse: {error}"));
    let retrying_client = "fictional-node-b"
        .parse::<ClientId>()
        .unwrap_or_else(|error| panic!("retrying client ID must parse: {error}"));

    let accepted = stage_package(&queue, RESULTS)
        .accept_for(first_client.clone())
        .unwrap_or_else(|error| panic!("authenticated package must be accepted: {error}"));
    let request_id = match accepted {
        EnqueueOutcome::Accepted { request_id, .. } => request_id,
        EnqueueOutcome::Existing { .. } => panic!("first request must be newly accepted"),
    };
    assert!(matches!(
        stage_package(&queue, RESULTS).accept_for(retrying_client),
        Ok(EnqueueOutcome::Existing { .. })
    ));

    let accepted_root = root
        .path()
        .join("queue/pending")
        .join(request_id.to_string());
    let validated = validate_accepted_package(&accepted_root, &PackagePolicy::default())
        .unwrap_or_else(|error| panic!("accepted package must revalidate: {error}"));
    assert_eq!(
        validated
            .acceptance()
            .and_then(|metadata| metadata.client_id.clone()),
        Some(first_client)
    );
}

#[test]
fn identical_retry_revalidates_the_existing_accepted_package() {
    let missing_metadata_root = TestDirectory::create();
    let queue = initialize_queue(missing_metadata_root.path(), PackagePolicy::default());
    let accepted = accept(stage_package(&queue, RESULTS));
    let request_id = match accepted {
        EnqueueOutcome::Accepted { request_id, .. } => request_id,
        EnqueueOutcome::Existing { .. } => panic!("first request must be newly accepted"),
    };
    let accepted_root = missing_metadata_root
        .path()
        .join("queue/pending")
        .join(request_id.to_string());
    if let Err(error) = fs::remove_file(accepted_root.join("acceptance.json")) {
        panic!("fixture acceptance metadata must be removed: {error}");
    }
    let error = match stage_package(&queue, RESULTS).accept() {
        Ok(_) => panic!("retry must not trust a package with missing acceptance metadata"),
        Err(error) => error,
    };
    assert!(matches!(error, QueueError::CorruptState { .. }));
    assert_eq!(error.error_code(), ErrorCode::ContentValidationFailed);

    let changed_payload_root = TestDirectory::create();
    let queue = initialize_queue(changed_payload_root.path(), PackagePolicy::default());
    let accepted = accept(stage_package(&queue, RESULTS));
    let request_id = match accepted {
        EnqueueOutcome::Accepted { request_id, .. } => request_id,
        EnqueueOutcome::Existing { .. } => panic!("first request must be newly accepted"),
    };
    let payload = changed_payload_root
        .path()
        .join("queue/pending")
        .join(request_id.to_string())
        .join("payload/benchmark/results.csv");
    if let Err(error) = fs::write(payload, b"step,value\n1,99\n") {
        panic!("fixture accepted payload must be changed: {error}");
    }
    let error = match stage_package(&queue, RESULTS).accept() {
        Ok(_) => panic!("retry must not trust changed immutable payload"),
        Err(error) => error,
    };
    assert!(matches!(error, QueueError::CorruptState { .. }));
    assert_eq!(error.error_code(), ErrorCode::ContentValidationFailed);

    let changed_digest_root = TestDirectory::create();
    let queue = initialize_queue(changed_digest_root.path(), PackagePolicy::default());
    let accepted = accept(stage_package(&queue, RESULTS));
    let request_id = match accepted {
        EnqueueOutcome::Accepted { request_id, .. } => request_id,
        EnqueueOutcome::Existing { .. } => panic!("first request must be newly accepted"),
    };
    let digest = changed_digest_root
        .path()
        .join("queue/pending")
        .join(request_id.to_string())
        .join("digest");
    if let Err(error) = fs::write(digest, format!("sha256:{}\n", "0".repeat(64))) {
        panic!("fixture accepted digest must be changed canonically: {error}");
    }
    let error = match stage_package(&queue, RESULTS).accept() {
        Ok(_) => panic!("retry must not classify a corrupt stored digest as request ID reuse"),
        Err(error) => error,
    };
    assert!(matches!(error, QueueError::CorruptState { .. }));
    assert_eq!(error.error_code(), ErrorCode::ContentValidationFailed);
}

#[test]
fn acceptance_sequence_is_monotonic_across_restart() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    let request_ids = ["01K00000000000000000000010", "01K00000000000000000000011"];

    for (index, request_id) in request_ids.into_iter().enumerate() {
        let outcome = accept(stage_package_with_request_id(&queue, RESULTS, request_id));
        let accepted_id = match outcome {
            EnqueueOutcome::Accepted { request_id, .. } => request_id,
            EnqueueOutcome::Existing { .. } => panic!("unique request must be newly accepted"),
        };
        let package = match validate_accepted_package(
            &root
                .path()
                .join("queue/pending")
                .join(accepted_id.to_string()),
            &PackagePolicy::default(),
        ) {
            Ok(package) => package,
            Err(error) => panic!("accepted package must validate: {error}"),
        };
        assert_eq!(
            package.acceptance().map(|metadata| metadata.sequence.get()),
            Some(index as u64 + 1)
        );
    }

    drop(queue);
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    let outcome = accept(stage_package_with_request_id(
        &queue,
        RESULTS,
        "01K00000000000000000000012",
    ));
    let accepted_id = match outcome {
        EnqueueOutcome::Accepted { request_id, .. } => request_id,
        EnqueueOutcome::Existing { .. } => panic!("unique request must be newly accepted"),
    };
    let package = match validate_accepted_package(
        &root
            .path()
            .join("queue/pending")
            .join(accepted_id.to_string()),
        &PackagePolicy::default(),
    ) {
        Ok(package) => package,
        Err(error) => panic!("accepted package must validate: {error}"),
    };
    assert_eq!(
        package.acceptance().map(|metadata| metadata.sequence.get()),
        Some(3)
    );
}

#[test]
fn missing_sequence_state_fails_closed_when_requests_exist() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    let _ = accept(stage_package(&queue, RESULTS));
    drop(queue);
    if let Err(error) = fs::remove_file(root.path().join("queue/next-sequence")) {
        panic!("fixture sequence state must be removed: {error}");
    }

    let error = match FileQueue::initialize(root.path().join("queue"), PackagePolicy::default()) {
        Ok(_) => panic!("missing sequence state with accepted requests must fail"),
        Err(error) => error,
    };
    assert!(matches!(error, QueueError::InvalidSequenceState));
    assert_eq!(error.error_code(), ErrorCode::InternalError);
    assert!(!root.path().join("queue/next-sequence").exists());
}

#[test]
fn missing_accepted_digest_is_permanent_corruption() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    let accepted = accept(stage_package(&queue, RESULTS));
    let request_id = match accepted {
        EnqueueOutcome::Accepted { request_id, .. } => request_id,
        EnqueueOutcome::Existing { .. } => panic!("first request must be newly accepted"),
    };
    let digest = root
        .path()
        .join("queue/pending")
        .join(request_id.to_string())
        .join("digest");
    if let Err(error) = fs::remove_file(digest) {
        panic!("fixture digest must be removed: {error}");
    }

    let error = match stage_package(&queue, RESULTS).accept() {
        Ok(_) => panic!("missing accepted digest must fail"),
        Err(error) => error,
    };
    assert!(matches!(error, QueueError::CorruptState { .. }));
    assert_eq!(error.error_code(), ErrorCode::ContentValidationFailed);
}

#[test]
fn rejects_request_id_reuse_with_different_contents() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    let _ = accept(stage_package(&queue, RESULTS));

    let error = match stage_package(&queue, b"step,value\n1,99\n").accept() {
        Ok(_) => panic!("different contents with the same request ID must fail"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        QueueError::RequestIdReused {
            state: QueueState::Pending,
            ..
        }
    ));
    assert_eq!(error.error_code(), ErrorCode::RequestIdReused);
    assert!(incoming_is_empty(root.path()));
}

#[test]
fn finds_idempotent_requests_after_a_state_transition() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    let accepted = accept(stage_package(&queue, RESULTS));
    let request_id = match accepted {
        EnqueueOutcome::Accepted { request_id, .. } => request_id,
        EnqueueOutcome::Existing { .. } => panic!("first request must be newly accepted"),
    };

    let pending = root
        .path()
        .join("queue/pending")
        .join(request_id.to_string());
    let processing = root
        .path()
        .join("queue/processing")
        .join(request_id.to_string());
    if let Err(error) = fs::rename(pending, processing) {
        panic!("fixture request must move to processing: {error}");
    }

    assert!(matches!(
        accept(stage_package(&queue, RESULTS)),
        EnqueueOutcome::Existing {
            state: QueueState::Processing,
            ..
        }
    ));
}

#[test]
fn rejects_streams_at_limits_without_leaving_partial_files() {
    let root = TestDirectory::create();
    let maximum_file_bytes = REQUEST_JSON.len() as u64 + 1;
    let policy = match PackagePolicy::new(
        PackageLimits {
            maximum_total_bytes: 4 * maximum_file_bytes,
            maximum_file_bytes,
            ..PackageLimits::default()
        },
        ["csv"],
    ) {
        Ok(policy) => policy,
        Err(error) => panic!("fixture policy must be valid: {error}"),
    };
    let queue = initialize_queue(root.path(), policy);
    let mut package = match queue.begin() {
        Ok(package) => package,
        Err(error) => panic!("incoming package must begin: {error}"),
    };
    if let Err(error) = package.write_request(REQUEST_JSON.as_bytes()) {
        panic!("fixture request must be written: {error}");
    }
    if let Err(error) = package.add_payload(payload_path("benchmark/index.md"), MARKDOWN) {
        panic!("fixture Markdown must be written: {error}");
    }

    let oversized = vec![b'x'; maximum_file_bytes as usize + 1];
    let error =
        match package.add_payload(payload_path("benchmark/results.csv"), oversized.as_slice()) {
            Ok(()) => panic!("oversized payload must fail"),
            Err(error) => error,
        };
    assert!(matches!(
        error,
        QueueError::LimitExceeded {
            limit: QueueLimit::IndividualFileBytes,
            ..
        }
    ));
    assert_eq!(error.error_code(), ErrorCode::LimitExceeded);

    if let Err(error) = package.add_payload(payload_path("benchmark/results.csv"), RESULTS) {
        panic!("payload path must remain reusable after a rejected stream: {error}");
    }
    let _ = accept(package);
}

#[test]
fn rejects_payload_prefix_collisions_in_both_orders() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    let mut file_first = match queue.begin() {
        Ok(package) => package,
        Err(error) => panic!("incoming package must begin: {error}"),
    };
    if let Err(error) = file_first.add_payload(payload_path("a"), b"fictional".as_slice()) {
        panic!("first payload file must be written: {error}");
    }
    let error = match file_first.add_payload(payload_path("a/b"), b"fictional".as_slice()) {
        Ok(()) => panic!("file-prefix collision must fail"),
        Err(error) => error,
    };
    assert!(matches!(error, QueueError::PayloadPrefixCollision(_)));
    assert_eq!(error.error_code(), ErrorCode::InvalidRequest);

    let mut directory_first = match queue.begin() {
        Ok(package) => package,
        Err(error) => panic!("incoming package must begin: {error}"),
    };
    if let Err(error) = directory_first.add_payload(payload_path("a/b"), b"fictional".as_slice()) {
        panic!("nested payload file must be written: {error}");
    }
    let error = match directory_first.add_payload(payload_path("a"), b"fictional".as_slice()) {
        Ok(()) => panic!("directory-prefix collision must fail"),
        Err(error) => error,
    };
    assert!(matches!(error, QueueError::PayloadPrefixCollision(_)));
    assert_eq!(error.error_code(), ErrorCode::InvalidRequest);
}

#[test]
fn enforces_directory_and_path_component_limits_before_writing() {
    let root = TestDirectory::create();
    let policy = match PackagePolicy::new(
        PackageLimits {
            maximum_directory_count: 0,
            maximum_path_components: 2,
            ..PackageLimits::default()
        },
        ["csv"],
    ) {
        Ok(policy) => policy,
        Err(error) => panic!("fixture policy must be valid: {error}"),
    };
    let queue = initialize_queue(root.path(), policy);
    let mut package = match queue.begin() {
        Ok(package) => package,
        Err(error) => panic!("incoming package must begin: {error}"),
    };

    let error = match package.add_payload(payload_path("a/b"), b"fictional".as_slice()) {
        Ok(()) => panic!("directory-count limit must fail"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        QueueError::LimitExceeded {
            limit: QueueLimit::DirectoryCount,
            ..
        }
    ));
    assert!(!package.staging_path.join("payload/a").exists());

    let error = match package.add_payload(payload_path("a/b/c"), b"fictional".as_slice()) {
        Ok(()) => panic!("path-component limit must fail"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        QueueError::LimitExceeded {
            limit: QueueLimit::PathComponents,
            ..
        }
    ));

    let entry_policy = match PackagePolicy::new(
        PackageLimits {
            maximum_entry_count: 1,
            ..PackageLimits::default()
        },
        ["csv"],
    ) {
        Ok(policy) => policy,
        Err(error) => panic!("fixture policy must be valid: {error}"),
    };
    let entry_root = root.path().join("entry-limit");
    fs::create_dir(&entry_root)
        .unwrap_or_else(|error| panic!("entry-limit queue parent must be created: {error}"));
    let entry_queue = initialize_queue(&entry_root, entry_policy);
    let mut entry_package = match entry_queue.begin() {
        Ok(package) => package,
        Err(error) => panic!("entry-limit package must begin: {error}"),
    };
    let error = match entry_package.add_payload(payload_path("a/b"), b"fictional".as_slice()) {
        Ok(()) => panic!("entry-count limit must fail"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        QueueError::LimitExceeded {
            limit: QueueLimit::EntryCount,
            ..
        }
    ));
    assert!(!entry_package.staging_path.join("payload/a").exists());
}

struct FailAfterRename;

impl AcceptanceHook for FailAfterRename {
    fn reached(&mut self, phase: AcceptancePhase) -> io::Result<()> {
        if phase == AcceptancePhase::Renamed {
            Err(io::Error::other("fictional crash after rename"))
        } else {
            Ok(())
        }
    }
}

struct FailAfterExistingDirectoriesSynchronized;

impl AcceptanceHook for FailAfterExistingDirectoriesSynchronized {
    fn reached(&mut self, phase: AcceptancePhase) -> io::Result<()> {
        if phase == AcceptancePhase::ExistingQueueDirectoriesSynchronized {
            Err(io::Error::other(
                "fictional interruption after existing-state synchronization",
            ))
        } else {
            Ok(())
        }
    }
}

struct ReplaceQueueAfterSynchronization {
    configured: PathBuf,
    detached: PathBuf,
}

impl AcceptanceHook for ReplaceQueueAfterSynchronization {
    fn reached(&mut self, phase: AcceptancePhase) -> io::Result<()> {
        if phase == AcceptancePhase::QueueDirectoriesSynchronized {
            fs::rename(&self.configured, &self.detached)?;
            FileQueue::initialize(&self.configured, PackagePolicy::default())
                .map_err(io::Error::other)?;
        }
        Ok(())
    }
}

#[test]
fn queue_replacement_before_acceptance_response_prevents_acknowledgement() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    let mut hook = ReplaceQueueAfterSynchronization {
        configured: root.path().join("queue"),
        detached: root.path().join("detached-queue"),
    };

    assert!(matches!(
        stage_package(&queue, RESULTS).accept_with_hook(None, &mut hook),
        Err(QueueError::InvalidQueueIdentity)
    ));
    assert!(
        fs::read_dir(root.path().join("queue/pending"))
            .map(|mut entries| entries.next().is_none())
            .unwrap_or(false)
    );
}

#[test]
fn retry_recovers_after_interruption_following_atomic_rename() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    let error = match stage_package(&queue, RESULTS).accept_with_hook(None, &mut FailAfterRename) {
        Ok(_) => panic!("injected interruption must fail the first response"),
        Err(error) => error,
    };
    assert!(matches!(error, QueueError::Io(_)));

    let error = match stage_package(&queue, RESULTS)
        .accept_with_hook(None, &mut FailAfterExistingDirectoriesSynchronized)
    {
        Ok(_) => panic!("injected interruption must fail the second response"),
        Err(error) => error,
    };
    assert!(matches!(error, QueueError::Io(_)));

    assert!(matches!(
        accept(stage_package(&queue, RESULTS)),
        EnqueueOutcome::Existing {
            state: QueueState::Pending,
            ..
        }
    ));
}

#[test]
fn concurrent_identical_requests_create_one_pending_package() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    let barrier = Arc::new(Barrier::new(2));

    let first_queue = queue.clone();
    let first_barrier = Arc::clone(&barrier);
    let first = thread::spawn(move || {
        let package = stage_package(&first_queue, RESULTS);
        first_barrier.wait();
        package.accept()
    });

    let second_queue = queue;
    let second_barrier = Arc::clone(&barrier);
    let second = thread::spawn(move || {
        let package = stage_package(&second_queue, RESULTS);
        second_barrier.wait();
        package.accept()
    });

    let first = match first.join() {
        Ok(result) => match result {
            Ok(outcome) => outcome,
            Err(error) => panic!("first concurrent request must succeed: {error}"),
        },
        Err(_) => panic!("first concurrent request thread must not panic"),
    };
    let second = match second.join() {
        Ok(result) => match result {
            Ok(outcome) => outcome,
            Err(error) => panic!("second concurrent request must succeed: {error}"),
        },
        Err(_) => panic!("second concurrent request thread must not panic"),
    };

    let accepted_count = [first, second]
        .into_iter()
        .filter(|outcome| matches!(outcome, EnqueueOutcome::Accepted { .. }))
        .count();
    let existing_count = [first, second]
        .into_iter()
        .filter(|outcome| matches!(outcome, EnqueueOutcome::Existing { .. }))
        .count();
    assert_eq!(accepted_count, 1);
    assert_eq!(existing_count, 1);
}

#[test]
fn concurrent_queue_initialization_shares_one_lock_file() {
    let root = TestDirectory::create();
    let barrier = Arc::new(Barrier::new(2));

    let first_root = root.path().to_path_buf();
    let first_barrier = Arc::clone(&barrier);
    let first = thread::spawn(move || {
        first_barrier.wait();
        FileQueue::initialize(first_root.join("queue"), PackagePolicy::default())
    });

    let second_root = root.path().to_path_buf();
    let second_barrier = Arc::clone(&barrier);
    let second = thread::spawn(move || {
        second_barrier.wait();
        FileQueue::initialize(second_root.join("queue"), PackagePolicy::default())
    });

    for result in [first.join(), second.join()] {
        match result {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => panic!("concurrent queue initialization must succeed: {error}"),
            Err(_) => panic!("queue initialization thread must not panic"),
        }
    }
    assert!(root.path().join("queue/.locks/queue.lock").is_file());
    assert!(
        root.path()
            .join("queue/.locks/repository-writer.lock")
            .is_file()
    );
}

#[test]
fn an_abandoned_incoming_directory_is_never_reported_as_accepted() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    let abandoned = stage_package(&queue, RESULTS);
    let abandoned_path = abandoned.staging_path.clone();
    std::mem::forget(abandoned);
    assert!(abandoned_path.is_dir());

    assert!(matches!(
        accept(stage_package(&queue, RESULTS)),
        EnqueueOutcome::Accepted { .. }
    ));
    assert!(abandoned_path.is_dir());
}

#[test]
fn staging_directory_is_not_visible_before_its_lease_is_held() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    let queue_lock = match queue.open_queue_lock() {
        Ok(lock) => lock,
        Err(error) => panic!("queue lock must open: {error}"),
    };
    if let Err(error) = queue_lock.lock() {
        panic!("queue lock fixture must be held: {error}");
    }
    let (started_sender, started_receiver) = mpsc::channel();
    let (result_sender, result_receiver) = mpsc::channel();
    let concurrent_queue = queue.clone();
    let thread = thread::spawn(move || {
        if started_sender.send(()).is_err() {
            return;
        }
        let _ = result_sender.send(concurrent_queue.begin());
    });
    if let Err(error) = started_receiver.recv() {
        panic!("begin thread must start: {error}");
    }
    match result_receiver.recv_timeout(Duration::from_millis(100)) {
        Err(RecvTimeoutError::Timeout) => {}
        Err(error) => panic!("begin result channel must remain connected: {error}"),
        Ok(_) => panic!("begin must wait for the queue lock before creating a directory"),
    }
    assert!(incoming_is_empty(root.path()));

    drop(queue_lock);
    let package = match result_receiver.recv_timeout(Duration::from_secs(5)) {
        Ok(Ok(package)) => package,
        Ok(Err(error)) => panic!("begin must succeed after queue lock release: {error}"),
        Err(error) => panic!("begin must finish after queue lock release: {error}"),
    };
    assert!(package.staging_path.is_dir());
    match thread.join() {
        Ok(()) => {}
        Err(_) => panic!("begin thread must not panic"),
    }
}

#[test]
fn stale_incoming_requires_quarantine_before_reaping() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    let active = match queue.begin() {
        Ok(package) => package,
        Err(error) => panic!("active incoming package must begin: {error}"),
    };
    let abandoned = root
        .path()
        .join("queue/incoming/.incoming-01K00000000000000000000999");
    if let Err(error) = fs::create_dir(&abandoned) {
        panic!("abandoned incoming fixture must be created: {error}");
    }

    let moved = match queue.quarantine_stale_incoming(std::time::Duration::ZERO, 100, 100) {
        Ok(moved) => moved,
        Err(error) => panic!("stale incoming quarantine must succeed: {error}"),
    };
    assert_eq!(moved, 1);
    assert!(active.staging_path.is_dir());
    assert!(!abandoned.exists());
    assert!(
        root.path()
            .join("queue/quarantine/.incoming-01K00000000000000000000999")
            .is_dir()
    );

    let removed = match queue.reap_quarantined_incoming(std::time::Duration::ZERO, 100, 100) {
        Ok(removed) => removed,
        Err(error) => panic!("quarantined incoming reap must succeed: {error}"),
    };
    assert_eq!(removed, 1);
    assert!(active.staging_path.is_dir());
}

#[test]
fn quarantine_retention_starts_when_the_directory_is_quarantined() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    let abandoned = root
        .path()
        .join("queue/incoming/.incoming-01K00000000000000000000998");
    if let Err(error) = fs::create_dir(&abandoned) {
        panic!("abandoned incoming fixture must be created: {error}");
    }
    if let Err(error) = fs::write(abandoned.join(".quarantined-at"), b"") {
        panic!("incomplete quarantine marker fixture must be written: {error}");
    }

    let moved = match queue.quarantine_stale_incoming(std::time::Duration::ZERO, 100, 100) {
        Ok(moved) => moved,
        Err(error) => panic!("stale incoming quarantine must succeed: {error}"),
    };
    assert_eq!(moved, 1);
    let quarantined = root
        .path()
        .join("queue/quarantine/.incoming-01K00000000000000000000998");
    let marker = match fs::read_to_string(quarantined.join(".quarantined-at")) {
        Ok(marker) => marker,
        Err(error) => panic!("completed quarantine marker must be readable: {error}"),
    };
    assert!(!marker.is_empty());

    let removed =
        match queue.reap_quarantined_incoming(std::time::Duration::from_secs(60), 100, 100) {
            Ok(removed) => removed,
            Err(error) => panic!("retention check must succeed: {error}"),
        };
    assert_eq!(removed, 0);
    assert!(quarantined.is_dir());
}

#[test]
fn reaping_repairs_an_incomplete_quarantine_marker_before_aging() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    let quarantined = root
        .path()
        .join("queue/quarantine/.incoming-01K00000000000000000000994");
    if let Err(error) = fs::create_dir(&quarantined) {
        panic!("quarantined fixture must be created: {error}");
    }
    if let Err(error) = fs::write(quarantined.join(".quarantined-at"), b"") {
        panic!("incomplete quarantine marker fixture must be written: {error}");
    }

    let removed = match queue.reap_quarantined_incoming(std::time::Duration::ZERO, 100, 100) {
        Ok(removed) => removed,
        Err(error) => panic!("incomplete marker repair must succeed: {error}"),
    };
    assert_eq!(removed, 0);
    let marker = match fs::read_to_string(quarantined.join(".quarantined-at")) {
        Ok(marker) => marker,
        Err(error) => panic!("repaired quarantine marker must be readable: {error}"),
    };
    assert!(!marker.is_empty());

    let removed = match queue.reap_quarantined_incoming(std::time::Duration::ZERO, 100, 100) {
        Ok(removed) => removed,
        Err(error) => panic!("completed marker reap must succeed: {error}"),
    };
    assert_eq!(removed, 1);
    assert!(!quarantined.exists());
}

#[test]
fn marker_repairs_consume_the_maintenance_action_budget() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    let quarantine_root = root.path().join("queue/quarantine");
    for suffix in ["00989", "00990", "00991"] {
        let path = quarantine_root.join(format!(".incoming-01K000000000000000000{suffix}"));
        if let Err(error) = fs::create_dir(&path) {
            panic!("quarantined fixture must be created: {error}");
        }
        if let Err(error) = fs::write(path.join(".quarantined-at"), b"") {
            panic!("incomplete marker fixture must be written: {error}");
        }
    }

    let removed = match queue.reap_quarantined_incoming(Duration::ZERO, 100, 1) {
        Ok(removed) => removed,
        Err(error) => panic!("budgeted marker repair must succeed: {error}"),
    };
    assert_eq!(removed, 0);
    let completed_markers = match fs::read_dir(&quarantine_root) {
        Ok(entries) => entries
            .filter_map(Result::ok)
            .filter(|entry| {
                fs::read(entry.path().join(".quarantined-at"))
                    .is_ok_and(|marker| !marker.is_empty())
            })
            .count(),
        Err(error) => panic!("quarantine fixtures must be readable: {error}"),
    };
    assert_eq!(completed_markers, 1);
}

#[test]
fn stale_maintenance_respects_scan_and_action_budgets() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    for suffix in ["00995", "00996", "00997"] {
        let path = root.path().join(format!(
            "queue/incoming/.incoming-01K000000000000000000{suffix}"
        ));
        if let Err(error) = fs::create_dir(path) {
            panic!("abandoned incoming fixture must be created: {error}");
        }
    }

    for _ in 0..3 {
        let moved = match queue.quarantine_stale_incoming(std::time::Duration::ZERO, 1, 1) {
            Ok(moved) => moved,
            Err(error) => panic!("budgeted quarantine must succeed: {error}"),
        };
        assert_eq!(moved, 1);
    }
    let remaining = match fs::read_dir(root.path().join("queue/incoming")) {
        Ok(entries) => entries.count(),
        Err(error) => panic!("incoming directory must be readable: {error}"),
    };
    assert_eq!(remaining, 0);
}

#[test]
fn bounded_directory_scanner_resumes_after_the_previous_entry() {
    let root = TestDirectory::create();
    let scan_root = root.path().join("scan");
    if let Err(error) = fs::create_dir(&scan_root) {
        panic!("scan fixture root must be created: {error}");
    }
    for suffix in ["00992", "00993"] {
        let path = scan_root.join(format!(".incoming-01K000000000000000000{suffix}"));
        if let Err(error) = fs::create_dir(path) {
            panic!("scan fixture directory must be created: {error}");
        }
    }
    let mut scanner = DirectoryScanner::default();
    let first = match inactive_stale_directories(
        &scan_root,
        Duration::ZERO,
        1,
        1,
        StaleAgeSource::Directory,
        &mut scanner,
    ) {
        Ok(candidates) => candidates,
        Err(error) => panic!("first bounded scan must succeed: {error}"),
    };
    let second = match inactive_stale_directories(
        &scan_root,
        Duration::ZERO,
        1,
        1,
        StaleAgeSource::Directory,
        &mut scanner,
    ) {
        Ok(candidates) => candidates,
        Err(error) => panic!("second bounded scan must succeed: {error}"),
    };
    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    assert_ne!(first, second);
}

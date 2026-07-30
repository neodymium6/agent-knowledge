use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use agent_knowledge_core::{ErrorCode, PayloadPath};

use super::{
    AcceptanceHook, AcceptancePhase, EnqueueOutcome, FileQueue, IncomingPackage, QueueError,
    QueueLimit, QueueState,
};
use crate::{PackageLimits, PackagePolicy};

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
    match FileQueue::initialize(root.join("queue"), root.join("locks/queue.lock"), policy) {
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

#[test]
fn retry_recovers_after_interruption_following_atomic_rename() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path(), PackagePolicy::default());
    let error = match stage_package(&queue, RESULTS).accept_with_hook(&mut FailAfterRename) {
        Ok(_) => panic!("injected interruption must fail the first response"),
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
        FileQueue::initialize(
            first_root.join("queue"),
            first_root.join("locks/queue.lock"),
            PackagePolicy::default(),
        )
    });

    let second_root = root.path().to_path_buf();
    let second_barrier = Arc::clone(&barrier);
    let second = thread::spawn(move || {
        second_barrier.wait();
        FileQueue::initialize(
            second_root.join("queue"),
            second_root.join("locks/queue.lock"),
            PackagePolicy::default(),
        )
    });

    for result in [first.join(), second.join()] {
        match result {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => panic!("concurrent queue initialization must succeed: {error}"),
            Err(_) => panic!("queue initialization thread must not panic"),
        }
    }
    assert!(root.path().join("locks/queue.lock").is_file());
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

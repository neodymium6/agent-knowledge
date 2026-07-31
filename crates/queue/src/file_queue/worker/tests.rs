use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use agent_knowledge_core::{BatchId, ErrorCode, PayloadPath, RequestId};

use super::{
    BatchClaimOutcome, ClaimHook, ClaimPhase, ProcessingScanOutcome, WorkerPhase, WorkerQueueError,
    WorkerResultRecord, WorkerResultStatus, WorkerSession, read_required_phase_record,
};
use crate::{EnqueueOutcome, FileQueue, PackagePolicy, QueueState};

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
const REQUEST_ID: &str = "01K00000000000000000000000";
const FIRST_BATCH_ID: &str = "01K00000000000000000000010";
const SECOND_BATCH_ID: &str = "01K00000000000000000000011";

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn create() -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "agent-knowledge-worker-queue-test-{}-{sequence}",
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

fn parse_request_id() -> RequestId {
    match REQUEST_ID.parse() {
        Ok(request_id) => request_id,
        Err(error) => panic!("fixture request ID must parse: {error}"),
    }
}

fn parse_batch_id(value: &str) -> BatchId {
    match value.parse() {
        Ok(batch_id) => batch_id,
        Err(error) => panic!("fixture batch ID must parse: {error}"),
    }
}

fn payload_path(value: &str) -> PayloadPath {
    match value.parse() {
        Ok(path) => path,
        Err(error) => panic!("fixture payload path must parse: {error}"),
    }
}

fn initialize_queue(root: &Path) -> FileQueue {
    match FileQueue::initialize(root.join("queue"), PackagePolicy::default()) {
        Ok(queue) => queue,
        Err(error) => panic!("fixture queue must initialize: {error}"),
    }
}

fn accept_fixture(queue: &FileQueue) {
    accept_fixture_with_id(queue, REQUEST_ID);
}

fn accept_fixture_with_id(queue: &FileQueue, request_id: &str) {
    let mut incoming = match queue.begin() {
        Ok(incoming) => incoming,
        Err(error) => panic!("fixture package must begin: {error}"),
    };
    let request = REQUEST_JSON.replace(REQUEST_ID, request_id);
    if let Err(error) = incoming.write_request(request.as_bytes()) {
        panic!("fixture request must be written: {error}");
    }
    let markdown = String::from_utf8_lossy(MARKDOWN).replace(REQUEST_ID, request_id);
    if let Err(error) =
        incoming.add_payload(payload_path("benchmark/index.md"), markdown.as_bytes())
    {
        panic!("fixture Markdown must be written: {error}");
    }
    match incoming.accept() {
        Ok(EnqueueOutcome::Accepted { .. }) => {}
        Ok(EnqueueOutcome::Existing { .. }) => panic!("fixture request must be newly accepted"),
        Err(error) => panic!("fixture request must be accepted: {error}"),
    }
}

fn open_unrecovered_worker(queue: &FileQueue) -> WorkerSession {
    match queue.try_worker_session() {
        Ok(worker) => worker,
        Err(error) => panic!("fixture Worker must acquire its lock: {error}"),
    }
}

fn open_worker(queue: &FileQueue) -> WorkerSession {
    let mut worker = open_unrecovered_worker(queue);
    match worker.scan_processing(16) {
        Ok(ProcessingScanOutcome::Complete { claims, .. }) if claims.is_empty() => worker,
        Ok(ProcessingScanOutcome::Complete { .. }) => {
            panic!("ordinary Worker fixture must not contain interrupted claims")
        }
        Ok(ProcessingScanOutcome::Scanning { .. }) => {
            panic!("ordinary Worker recovery must fit in one test scan")
        }
        Err(error) => panic!("ordinary Worker recovery must complete: {error}"),
    }
}

#[test]
fn claims_pending_package_with_durable_phase_record() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path());
    accept_fixture(&queue);
    let request_id = parse_request_id();
    let batch_id = parse_batch_id(FIRST_BATCH_ID);
    let mut worker = open_unrecovered_worker(&queue);

    let error = match worker.claim(request_id, batch_id) {
        Ok(_) => panic!("new Worker must recover processing before claiming"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        WorkerQueueError::ProcessingRecoveryRequired
    ));
    match worker.scan_processing(10) {
        Ok(ProcessingScanOutcome::Complete { claims, .. }) => assert!(claims.is_empty()),
        Ok(ProcessingScanOutcome::Scanning { .. }) => {
            panic!("empty processing recovery must complete immediately")
        }
        Err(error) => panic!("empty processing recovery must succeed: {error}"),
    }
    let claimed = match worker.claim(request_id, batch_id) {
        Ok(claimed) => claimed,
        Err(error) => panic!("pending request must be claimed: {error}"),
    };

    assert_eq!(claimed.token().request_id(), request_id);
    assert_eq!(claimed.token().batch_id(), batch_id);
    assert_eq!(claimed.token().attempt().get(), 1);
    assert_eq!(claimed.package().request().request_id, request_id);
    assert!(
        !root
            .path()
            .join(format!("queue/pending/{request_id}"))
            .exists()
    );
    let processing = root.path().join(format!("queue/processing/{request_id}"));
    assert!(processing.is_dir());
    let record = match read_required_phase_record(&processing, request_id, QueueState::Processing) {
        Ok(record) => record,
        Err(error) => panic!("durable claim record must validate: {error}"),
    };
    assert_eq!(record.batch_id, batch_id);
    assert_eq!(record.attempt.get(), 1);
    assert_eq!(record.phase, WorkerPhase::Claimed);
}

#[test]
fn requeue_requires_current_token_and_increments_the_next_attempt() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path());
    accept_fixture(&queue);
    let request_id = parse_request_id();
    let first_batch = parse_batch_id(FIRST_BATCH_ID);
    let second_batch = parse_batch_id(SECOND_BATCH_ID);
    let first = match queue.claim(request_id, first_batch) {
        Ok(claimed) => claimed.token(),
        Err(error) => panic!("first claim must succeed: {error}"),
    };
    if let Err(error) = queue.requeue_claimed(first) {
        panic!("current claim must be requeued: {error}");
    }

    let second = match queue.claim(request_id, second_batch) {
        Ok(claimed) => claimed.token(),
        Err(error) => panic!("second claim must succeed: {error}"),
    };
    assert_eq!(second.attempt().get(), 2);
    let error = match queue.requeue_claimed(first) {
        Ok(()) => panic!("stale claim token must not requeue a later attempt"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        WorkerQueueError::ClaimChanged {
            request_id: changed
        } if changed == request_id
    ));
    assert_eq!(error.error_code(), ErrorCode::InvalidRequest);
    if let Err(error) = queue.requeue_claimed(second) {
        panic!("latest claim must be requeued: {error}");
    }
}

#[test]
fn claim_rejects_corrupt_stored_phase_metadata() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path());
    accept_fixture(&queue);
    let request_id = parse_request_id();
    let phase = root
        .path()
        .join(format!("queue/pending/{request_id}/phase.json"));
    if let Err(error) = fs::write(phase, b"{not-json}\n") {
        panic!("corrupt phase fixture must be written: {error}");
    }

    let error = match queue.claim(request_id, parse_batch_id(FIRST_BATCH_ID)) {
        Ok(_) => panic!("corrupt phase metadata must reject claim"),
        Err(error) => error,
    };
    assert!(matches!(error, WorkerQueueError::InvalidPhaseMetadata(_)));
    assert_eq!(error.error_code(), ErrorCode::ContentValidationFailed);
    assert!(
        root.path()
            .join(format!("queue/pending/{request_id}"))
            .is_dir()
    );
}

struct FailAtPhase {
    phase: ClaimPhase,
}

impl ClaimHook for FailAtPhase {
    fn reached(&mut self, phase: ClaimPhase) -> io::Result<()> {
        if phase == self.phase {
            return Err(io::Error::other("injected claim interruption"));
        }
        Ok(())
    }
}

#[test]
fn retry_after_phase_write_advances_attempt_before_claiming() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path());
    accept_fixture(&queue);
    let request_id = parse_request_id();
    let first_batch = parse_batch_id(FIRST_BATCH_ID);
    let error = match queue.claim_with_hook(
        request_id,
        first_batch,
        &mut FailAtPhase {
            phase: ClaimPhase::PhaseSynchronized,
        },
    ) {
        Ok(_) => panic!("injected interruption must fail the first claim"),
        Err(error) => error,
    };
    assert_eq!(error.error_code(), ErrorCode::TemporaryFailure);
    assert!(
        root.path()
            .join(format!("queue/pending/{request_id}"))
            .is_dir()
    );

    let claimed = match queue.claim(request_id, parse_batch_id(SECOND_BATCH_ID)) {
        Ok(claimed) => claimed,
        Err(error) => panic!("claim retry must succeed: {error}"),
    };
    assert_eq!(claimed.token().attempt().get(), 2);
}

#[test]
fn rename_interruption_can_be_recovered_with_the_durable_claim() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path());
    accept_fixture(&queue);
    let request_id = parse_request_id();
    let batch_id = parse_batch_id(FIRST_BATCH_ID);
    let error = match queue.claim_with_hook(
        request_id,
        batch_id,
        &mut FailAtPhase {
            phase: ClaimPhase::Renamed,
        },
    ) {
        Ok(_) => panic!("injected interruption must fail the claim"),
        Err(error) => error,
    };
    assert_eq!(error.error_code(), ErrorCode::TemporaryFailure);
    let mut worker = open_unrecovered_worker(&queue);
    let recovered = match worker.scan_processing(10) {
        Ok(ProcessingScanOutcome::Complete { claims, .. }) => claims,
        Ok(ProcessingScanOutcome::Scanning { .. }) => {
            panic!("single processing request must fit in the recovery scan")
        }
        Err(error) => panic!("public recovery scan must discover the durable claim: {error}"),
    };
    assert_eq!(recovered.len(), 1);
    let token = recovered[0].token();
    assert_eq!(token.request_id(), request_id);
    assert_eq!(token.batch_id(), batch_id);
    if let Err(error) = worker.requeue_claimed(token) {
        panic!("interrupted renamed request must be recoverable: {error}");
    }
    assert!(
        root.path()
            .join(format!("queue/pending/{request_id}"))
            .is_dir()
    );
}

#[test]
fn concurrent_claimers_produce_one_processing_owner() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path());
    accept_fixture(&queue);
    let request_id = parse_request_id();
    let barrier = Arc::new(Barrier::new(2));

    let first_queue = queue.clone();
    let first_barrier = Arc::clone(&barrier);
    let first = thread::spawn(move || {
        first_barrier.wait();
        first_queue.claim(request_id, parse_batch_id(FIRST_BATCH_ID))
    });
    let second_queue = queue.clone();
    let second_barrier = Arc::clone(&barrier);
    let second = thread::spawn(move || {
        second_barrier.wait();
        second_queue.claim(request_id, parse_batch_id(SECOND_BATCH_ID))
    });

    let results = [first.join(), second.join()];
    let successes = results
        .iter()
        .filter(|result| matches!(result, Ok(Ok(_))))
        .count();
    let invalid_states = results
        .iter()
        .filter(|result| {
            matches!(
                result,
                Ok(Err(WorkerQueueError::InvalidState {
                    expected: QueueState::Pending,
                    actual: QueueState::Processing,
                    ..
                }))
            )
        })
        .count();
    assert_eq!(successes, 1);
    assert_eq!(invalid_states, 1);
}

#[test]
fn bounded_batch_claims_globally_earliest_acceptance_sequences() {
    const FIRST_ACCEPTED: &str = "01K00000000000000000000022";
    const SECOND_ACCEPTED: &str = "01K00000000000000000000020";
    const THIRD_ACCEPTED: &str = "01K00000000000000000000021";

    let root = TestDirectory::create();
    let queue = initialize_queue(root.path());
    accept_fixture_with_id(&queue, FIRST_ACCEPTED);
    accept_fixture_with_id(&queue, SECOND_ACCEPTED);
    accept_fixture_with_id(&queue, THIRD_ACCEPTED);
    let mut worker = open_worker(&queue);
    let batch_id = parse_batch_id(FIRST_BATCH_ID);

    let claimed = loop {
        match worker.claim_next_batch(batch_id, 1, 2) {
            Ok(BatchClaimOutcome::Scanning {
                scanned_entries,
                retained_candidates,
            }) => {
                assert_eq!(scanned_entries, 1);
                assert!(retained_candidates <= 2);
            }
            Ok(BatchClaimOutcome::Claimed(claimed)) => break claimed,
            Err(error) => panic!("bounded batch scan must succeed: {error}"),
        }
    };

    let claimed_ids: Vec<_> = claimed
        .iter()
        .map(|package| package.token().request_id().to_string())
        .collect();
    assert_eq!(claimed_ids, [FIRST_ACCEPTED, SECOND_ACCEPTED]);
    assert!(
        root.path()
            .join(format!("queue/pending/{THIRD_ACCEPTED}"))
            .is_dir()
    );
}

#[test]
fn batch_snapshot_excludes_requests_accepted_during_its_scan() {
    const SECOND_INITIAL: &str = "01K00000000000000000000020";
    const LATER_ACCEPTED: &str = "01K00000000000000000000021";

    let root = TestDirectory::create();
    let queue = initialize_queue(root.path());
    accept_fixture(&queue);
    accept_fixture_with_id(&queue, SECOND_INITIAL);
    let mut worker = open_worker(&queue);
    let batch_id = parse_batch_id(FIRST_BATCH_ID);
    match worker.claim_next_batch(batch_id, 1, 10) {
        Ok(BatchClaimOutcome::Scanning { .. }) => {}
        Ok(BatchClaimOutcome::Claimed(_)) => panic!("one-entry scan must remain resumable"),
        Err(error) => panic!("first scan step must succeed: {error}"),
    }

    accept_fixture_with_id(&queue, LATER_ACCEPTED);
    let claimed = loop {
        match worker.claim_next_batch(batch_id, 1, 10) {
            Ok(BatchClaimOutcome::Scanning { .. }) => {}
            Ok(BatchClaimOutcome::Claimed(claimed)) => break claimed,
            Err(error) => panic!("snapshot scan must finish: {error}"),
        }
    };

    assert_eq!(claimed.len(), 2);
    assert!(
        root.path()
            .join(format!("queue/pending/{LATER_ACCEPTED}"))
            .is_dir()
    );
}

#[test]
fn corrupt_pending_request_is_failed_without_blocking_valid_candidates() {
    const VALID_REQUEST_ID: &str = "01K00000000000000000000020";

    let root = TestDirectory::create();
    let queue = initialize_queue(root.path());
    accept_fixture(&queue);
    accept_fixture_with_id(&queue, VALID_REQUEST_ID);
    let corrupt_request_id = parse_request_id();
    let corrupt_digest = root
        .path()
        .join(format!("queue/pending/{corrupt_request_id}/digest"));
    if let Err(error) = fs::write(corrupt_digest, b"not-a-digest\n") {
        panic!("corrupt package fixture must be written: {error}");
    }
    let mut worker = open_worker(&queue);
    let batch_id = parse_batch_id(FIRST_BATCH_ID);

    let claimed = loop {
        match worker.claim_next_batch(batch_id, 1, 10) {
            Ok(BatchClaimOutcome::Scanning { .. }) => {}
            Ok(BatchClaimOutcome::Claimed(claimed)) => break claimed,
            Err(error) => panic!("corrupt request must not block the valid batch: {error}"),
        }
    };

    assert_eq!(claimed.len(), 1);
    assert_eq!(
        claimed[0].token().request_id().to_string(),
        VALID_REQUEST_ID
    );
    let result_path = root
        .path()
        .join(format!("queue/failed/{corrupt_request_id}/result.json"));
    let result_bytes = match fs::read(result_path) {
        Ok(bytes) => bytes,
        Err(error) => panic!("failed request result must be readable: {error}"),
    };
    let result: WorkerResultRecord = match serde_json::from_slice(&result_bytes) {
        Ok(result) => result,
        Err(error) => panic!("failed request result must decode: {error}"),
    };
    assert_eq!(result.request_id, corrupt_request_id);
    assert_eq!(result.batch_id, batch_id);
    assert_eq!(result.status, WorkerResultStatus::Failed);
    assert_eq!(result.error_code, ErrorCode::ContentValidationFailed);
}

#[test]
fn worker_writer_lock_is_exclusive_and_released_on_drop() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path());
    let independently_initialized = initialize_queue(root.path());
    let worker = match queue.try_worker_session() {
        Ok(worker) => worker,
        Err(error) => panic!("first Worker must acquire the writer lock: {error}"),
    };

    let error = match independently_initialized.try_worker_session() {
        Ok(_) => panic!("second Worker must not share the writer lock"),
        Err(error) => error,
    };
    assert!(matches!(error, WorkerQueueError::WorkerAlreadyRunning));
    assert_eq!(error.error_code(), ErrorCode::TemporaryFailure);

    drop(worker);
    if let Err(error) = independently_initialized.try_worker_session() {
        panic!("dropping the Worker must release its writer lock: {error}");
    }
}

#[test]
fn active_batch_scan_rejects_changed_identity_or_capacity() {
    const OTHER_REQUEST_ID: &str = "01K00000000000000000000020";

    let root = TestDirectory::create();
    let queue = initialize_queue(root.path());
    accept_fixture(&queue);
    accept_fixture_with_id(&queue, OTHER_REQUEST_ID);
    let mut worker = open_worker(&queue);
    let first_batch = parse_batch_id(FIRST_BATCH_ID);
    match worker.claim_next_batch(first_batch, 1, 1) {
        Ok(BatchClaimOutcome::Scanning { .. }) => {}
        Ok(BatchClaimOutcome::Claimed(_)) => panic!("one-entry scan must remain resumable"),
        Err(error) => panic!("first scan step must succeed: {error}"),
    }

    let error = match worker.claim_next_batch(parse_batch_id(SECOND_BATCH_ID), 1, 1) {
        Ok(_) => panic!("active scan must retain its batch identity"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        WorkerQueueError::BatchScanChanged {
            active_batch_id
        } if active_batch_id == first_batch
    ));

    let error = match worker.claim_next_batch(first_batch, 1, 2) {
        Ok(_) => panic!("active scan must retain its request capacity"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        WorkerQueueError::BatchScanChanged {
            active_batch_id
        } if active_batch_id == first_batch
    ));

    let error = match worker.claim(parse_request_id(), first_batch) {
        Ok(_) => panic!("exact claim must not mutate an active batch snapshot"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        WorkerQueueError::BatchScanInProgress {
            active_batch_id
        } if active_batch_id == first_batch
    ));
}

#[test]
fn batch_scan_rejects_zero_limits() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path());
    let mut worker = open_worker(&queue);
    let batch_id = parse_batch_id(FIRST_BATCH_ID);

    for (scan_limit, request_limit) in [(0, 1), (1, 0)] {
        let error = match worker.claim_next_batch(batch_id, scan_limit, request_limit) {
            Ok(_) => panic!("zero batch limit must be rejected"),
            Err(error) => error,
        };
        assert!(matches!(error, WorkerQueueError::InvalidBatchLimits));
        assert_eq!(error.error_code(), ErrorCode::InvalidRequest);
    }
}

#[test]
fn processing_recovery_is_bounded_and_blocks_mutation_until_complete() {
    const OTHER_REQUEST_ID: &str = "01K00000000000000000000020";

    let root = TestDirectory::create();
    let queue = initialize_queue(root.path());
    accept_fixture(&queue);
    accept_fixture_with_id(&queue, OTHER_REQUEST_ID);
    let batch_id = parse_batch_id(FIRST_BATCH_ID);
    if let Err(error) = queue.claim(parse_request_id(), batch_id) {
        panic!("first recovery fixture must be claimed: {error}");
    }
    let other_request_id = match OTHER_REQUEST_ID.parse() {
        Ok(request_id) => request_id,
        Err(error) => panic!("second recovery request ID must parse: {error}"),
    };
    if let Err(error) = queue.claim(other_request_id, batch_id) {
        panic!("second recovery fixture must be claimed: {error}");
    }
    let mut worker = open_unrecovered_worker(&queue);

    let mut recovered = match worker.scan_processing(1) {
        Ok(ProcessingScanOutcome::Scanning {
            scanned_entries,
            claims,
        }) => {
            assert_eq!(scanned_entries, 1);
            claims
        }
        Ok(ProcessingScanOutcome::Complete { .. }) => {
            panic!("one-entry recovery step must remain resumable")
        }
        Err(error) => panic!("first recovery step must succeed: {error}"),
    };
    let error = match worker.requeue_claimed(recovered[0].token()) {
        Ok(()) => panic!("processing mutation must wait for recovery scan completion"),
        Err(error) => error,
    };
    assert!(matches!(error, WorkerQueueError::ProcessingScanInProgress));

    loop {
        match worker.scan_processing(1) {
            Ok(ProcessingScanOutcome::Scanning { claims, .. }) => recovered.extend(claims),
            Ok(ProcessingScanOutcome::Complete { claims, .. }) => {
                recovered.extend(claims);
                break;
            }
            Err(error) => panic!("processing recovery must finish: {error}"),
        }
    }
    assert_eq!(recovered.len(), 2);
    for claim in recovered {
        if let Err(error) = worker.requeue_claimed(claim.token()) {
            panic!("recovered claim must be requeueable: {error}");
        }
    }
}

#[test]
fn processing_recovery_rejects_zero_scan_limit() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path());
    let mut worker = open_worker(&queue);

    let error = match worker.scan_processing(0) {
        Ok(_) => panic!("zero recovery scan limit must be rejected"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        WorkerQueueError::InvalidProcessingScanLimit
    ));
    assert_eq!(error.error_code(), ErrorCode::InvalidRequest);
}

#[test]
fn processing_recovery_error_keeps_worker_transitions_gated() {
    let root = TestDirectory::create();
    let queue = initialize_queue(root.path());
    accept_fixture(&queue);
    let invalid_entry = root.path().join("queue/processing/not-a-request");
    if let Err(error) = fs::write(invalid_entry, b"invalid processing entry\n") {
        panic!("invalid processing fixture must be written: {error}");
    }
    let mut worker = open_unrecovered_worker(&queue);

    let error = match worker.scan_processing(10) {
        Ok(_) => panic!("invalid processing entry must fail recovery"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        WorkerQueueError::InvalidProcessingEntry { .. }
    ));

    let error = match worker.claim_next_batch(parse_batch_id(FIRST_BATCH_ID), 10, 10) {
        Ok(_) => panic!("failed recovery must keep batch claims gated"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        WorkerQueueError::ProcessingRecoveryRequired
    ));
}

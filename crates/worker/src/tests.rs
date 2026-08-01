use std::cell::RefCell;
use std::ffi::OsString;
use std::fs;
use std::io::Cursor;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use agent_knowledge_core::{BatchId, ErrorCode, PayloadPath, RequestId, Revision};
use agent_knowledge_queue::{
    ClaimedPackage, FileQueue, PackagePolicy, ProcessingScanOutcome, QueueState, WorkerSession,
};
use agent_knowledge_release::{QuartzBuilder, ReleasePolicy, ReleaseStore};
use agent_knowledge_repository::{
    BatchCommitOutcome, BatchPublication, ContentPolicy, GitIdentity, GitRepository,
    GitTransactionError, PublicationError,
};
use time::{Duration as TimeDuration, OffsetDateTime};

use super::{
    BatchCloseReason, BatchProcessor, BatchSchedule, StartupOutcome, WorkerPollOutcome,
    WorkerRunError, WorkerRunLimits, WorkerRunOutcome, WorkerRuntime,
};

const REQUEST_ID: &str = "01K00000000000000000000001";
const DOCUMENT_ID: &str = "01K00000000000000000000002";
const BATCH_ID: &str = "01K00000000000000000000003";
const SECOND_REQUEST_ID: &str = "01K00000000000000000000004";
const SECOND_DOCUMENT_ID: &str = "01K00000000000000000000005";
const THIRD_REQUEST_ID: &str = "01K00000000000000000000006";
const THIRD_DOCUMENT_ID: &str = "01K00000000000000000000007";
static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn create() -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "agent-knowledge-worker-transaction-test-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap_or_else(|error| panic!("test root must be created: {error}"));
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
            panic!("test root must be removed: {error}");
        }
    }
}

struct Fixture {
    root: TestDirectory,
    repository_path: PathBuf,
    content_path: PathBuf,
    work_path: PathBuf,
    quartz: QuartzBuilder,
    releases: ReleaseStore,
}

impl Fixture {
    fn create() -> Self {
        Self::create_with_quartz_script(
            b"content=\noutput=\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = \"-d\" ]; then content=$2; shift 2; elif [ \"$1\" = \"-o\" ]; then output=$2; shift 2; else shift; fi\ndone\nprintf '%s\\n' '<p>fictional site</p>' > \"$output/index.html\"\n",
        )
    }

    fn create_with_quartz_script(script_contents: &[u8]) -> Self {
        let root = TestDirectory::create();
        let repository_path = root.path().join("repository");
        let seed_path = root.path().join("seed");
        let content_path = root.path().join("content");
        let work_path = root.path().join("work");
        run_git(
            None,
            ["init", "--bare", "--initial-branch=main"],
            Some(&repository_path),
        );
        run_git(None, ["init", "--initial-branch=main"], Some(&seed_path));
        run_git(
            Some(&seed_path),
            ["config", "user.name", "Fictional Test Author"],
            None,
        );
        run_git(
            Some(&seed_path),
            ["config", "user.email", "worker@example.invalid"],
            None,
        );
        run_git(
            Some(&seed_path),
            [
                "commit",
                "--allow-empty",
                "-m",
                "Initialize fictional knowledge",
            ],
            None,
        );
        run_git(
            Some(&seed_path),
            ["remote", "add", "origin"],
            Some(&repository_path),
        );
        run_git(Some(&seed_path), ["push", "origin", "main"], None);
        let status = Command::new("git")
            .arg(format!("--git-dir={}", repository_path.display()))
            .args(["worktree", "add"])
            .arg(&content_path)
            .arg("main")
            .status()
            .unwrap_or_else(|error| panic!("canonical worktree command must run: {error}"));
        assert!(status.success());
        fs::create_dir(&work_path)
            .unwrap_or_else(|error| panic!("work root must be created: {error}"));

        let integration = root.path().join("quartz-integration");
        fs::create_dir(&integration)
            .unwrap_or_else(|error| panic!("Quartz integration must be created: {error}"));
        let script = integration.join("quartz.sh");
        fs::write(&script, script_contents)
            .unwrap_or_else(|error| panic!("Quartz fixture must be written: {error}"));
        let quartz = QuartzBuilder::new(
            "/bin/sh",
            &integration,
            vec![OsString::from(script)],
            Duration::from_secs(5),
        )
        .unwrap_or_else(|error| panic!("Quartz fixture must initialize: {error}"));
        let releases = ReleaseStore::open(root.path().join("releases"), ReleasePolicy::default())
            .unwrap_or_else(|error| panic!("release store must initialize: {error}"));
        Self {
            root,
            repository_path,
            content_path,
            work_path,
            quartz,
            releases,
        }
    }

    fn repository(&self) -> GitRepository {
        let identity = GitIdentity::new("Agent Knowledge Worker", "worker@example.invalid")
            .unwrap_or_else(|error| panic!("Git identity must validate: {error}"));
        GitRepository::open(
            &self.repository_path,
            &self.content_path,
            &self.work_path,
            "main",
            identity,
        )
        .unwrap_or_else(|error| panic!("repository must open: {error}"))
    }

    fn queue_and_worker(&self) -> (FileQueue, WorkerSession) {
        let queue = FileQueue::initialize(self.root.path().join("queue"), PackagePolicy::default())
            .unwrap_or_else(|error| panic!("queue must initialize: {error}"));
        let mut worker = queue
            .try_worker_session()
            .unwrap_or_else(|error| panic!("Worker session must open: {error}"));
        match worker.scan_processing(16) {
            Ok(ProcessingScanOutcome::Complete { claims, .. }) if claims.is_empty() => {}
            Ok(_) => panic!("new queue must complete empty recovery"),
            Err(error) => panic!("new queue recovery must succeed: {error}"),
        }
        (queue, worker)
    }

    fn processor(&self, repository: GitRepository) -> BatchProcessor {
        BatchProcessor::new(
            repository,
            self.quartz.clone(),
            self.releases.clone(),
            ContentPolicy::default(),
            PackagePolicy::default(),
        )
    }
}

fn run_git<const N: usize>(working: Option<&Path>, arguments: [&str; N], path: Option<&Path>) {
    let mut command = Command::new("git");
    if let Some(working) = working {
        command.current_dir(working);
    }
    command.args(arguments);
    if let Some(path) = path {
        command.arg(path);
    }
    let status = command
        .status()
        .unwrap_or_else(|error| panic!("Git fixture command must run: {error}"));
    assert!(status.success());
}

fn batch_id() -> BatchId {
    BATCH_ID
        .parse()
        .unwrap_or_else(|error| panic!("batch ID must parse: {error}"))
}

fn request_id() -> RequestId {
    REQUEST_ID
        .parse()
        .unwrap_or_else(|error| panic!("request ID must parse: {error}"))
}

fn created_at() -> OffsetDateTime {
    OffsetDateTime::parse(
        "2026-07-31T04:00:00Z",
        &time::format_description::well_known::Rfc3339,
    )
    .unwrap_or_else(|error| panic!("timestamp must parse: {error}"))
}

fn enqueue_and_claim(queue: &FileQueue, worker: &mut WorkerSession) -> ClaimedPackage {
    enqueue_create(queue);
    worker
        .claim(request_id(), batch_id())
        .unwrap_or_else(|error| panic!("package must be claimed: {error}"))
}

fn enqueue_create(queue: &FileQueue) {
    enqueue_create_with_ids(queue, REQUEST_ID, DOCUMENT_ID);
}

fn enqueue_create_with_ids(queue: &FileQueue, request_id: &str, document_id: &str) {
    let request = format!(
        "{{\n\
         \"protocol_version\": 1,\n\
         \"request_id\": \"{request_id}\",\n\
         \"title\": \"Create a fictional experiment\",\n\
         \"project\": \"fictional-project\",\n\
         \"document_type\": \"experiment\",\n\
         \"created_at\": \"2026-07-31T04:00:00Z\",\n\
         \"operations\": [{{\n\
           \"type\": \"create_document\",\n\
         \"document_id\": \"{document_id}\",\n\
           \"content\": \"index.md\"\n\
         }}]\n\
         }}\n"
    );
    let markdown = format!(
        "---\n\
         schema_version: 1\n\
         document_id: {document_id}\n\
         title: Fictional experiment\n\
         created: 2026-07-31T03:50:00Z\n\
         request_id: {request_id}\n\
         status: active\n\
         ---\n\
         Fictional transaction body.\n"
    );
    let mut incoming = queue
        .begin()
        .unwrap_or_else(|error| panic!("package must begin: {error}"));
    incoming
        .write_request(Cursor::new(request.as_bytes()))
        .unwrap_or_else(|error| panic!("request must be staged: {error}"));
    let payload: PayloadPath = "index.md"
        .parse()
        .unwrap_or_else(|error| panic!("payload path must parse: {error}"));
    incoming
        .add_payload(payload, Cursor::new(markdown.as_bytes()))
        .unwrap_or_else(|error| panic!("payload must be staged: {error}"));
    incoming
        .accept()
        .unwrap_or_else(|error| panic!("package must be accepted: {error}"));
}

fn enqueue_missing_update(queue: &FileQueue, worker: &mut WorkerSession) -> ClaimedPackage {
    let revision = Revision::from_bytes([7; 32]);
    let request = format!(
        "{{\n\
         \"protocol_version\": 1,\n\
         \"request_id\": \"{REQUEST_ID}\",\n\
         \"title\": \"Update a missing fictional runbook\",\n\
         \"project\": \"fictional-project\",\n\
         \"document_type\": \"runbook\",\n\
         \"created_at\": \"2026-07-31T04:00:00Z\",\n\
         \"operations\": [{{\n\
           \"type\": \"update_document\",\n\
           \"document_id\": \"{DOCUMENT_ID}\",\n\
           \"expected_revision\": \"{revision}\",\n\
           \"content\": \"index.md\"\n\
         }}]\n\
         }}\n"
    );
    let markdown = format!(
        "---\n\
         schema_version: 1\n\
         document_id: {DOCUMENT_ID}\n\
         title: Missing fictional runbook\n\
         created: 2026-07-31T03:50:00Z\n\
         updated: 2026-07-31T04:00:00Z\n\
         request_id: {REQUEST_ID}\n\
         status: active\n\
         ---\n\
         Fictional update.\n"
    );
    let mut incoming = queue
        .begin()
        .unwrap_or_else(|error| panic!("package must begin: {error}"));
    incoming
        .write_request(Cursor::new(request.as_bytes()))
        .unwrap_or_else(|error| panic!("request must be staged: {error}"));
    let payload: PayloadPath = "index.md"
        .parse()
        .unwrap_or_else(|error| panic!("payload path must parse: {error}"));
    incoming
        .add_payload(payload, Cursor::new(markdown.as_bytes()))
        .unwrap_or_else(|error| panic!("payload must be staged: {error}"));
    incoming
        .accept()
        .unwrap_or_else(|error| panic!("package must be accepted: {error}"));
    worker
        .claim(request_id(), batch_id())
        .unwrap_or_else(|error| panic!("package must be claimed: {error}"))
}

#[test]
fn publishes_release_before_completing_the_queue_and_journal() {
    let fixture = Fixture::create();
    let repository = fixture.repository();
    let (queue, mut worker) = fixture.queue_and_worker();
    let claim = enqueue_and_claim(&queue, &mut worker);
    let processor = fixture.processor(repository);

    let outcome = processor
        .process(&mut worker, batch_id(), &[claim], created_at())
        .unwrap_or_else(|error| panic!("batch must publish: {error}"));
    let BatchCommitOutcome::Committed { commit, .. } = outcome else {
        panic!("create request must commit");
    };
    assert_eq!(
        fixture
            .releases
            .active_release()
            .unwrap_or_else(|error| panic!("active release must validate: {error}"))
            .unwrap_or_else(|| panic!("release must be active"))
            .commit(),
        commit
    );
    assert!(
        fixture
            .root
            .path()
            .join(format!("queue/{}/{}", QueueState::Completed, REQUEST_ID))
            .is_dir()
    );
    assert!(
        fixture
            .content_path
            .join(format!(
                "projects/fictional-project/experiments/2026-07-31-{DOCUMENT_ID}/index.md"
            ))
            .is_file()
    );
    assert_eq!(
        fs::read_dir(fixture.work_path.join("transactions"))
            .unwrap_or_else(|error| panic!("transactions must be readable: {error}"))
            .count(),
        0
    );
}

#[test]
fn recovery_prepares_release_before_advancing_an_interrupted_commit() {
    let fixture = Fixture::create();
    let repository = fixture.repository();
    let (queue, mut worker) = fixture.queue_and_worker();
    let claim = enqueue_and_claim(&queue, &mut worker);
    let built = RefCell::new(None);
    let result = repository.apply_batch_with_publication(
        &mut worker,
        batch_id(),
        std::slice::from_ref(&claim),
        ContentPolicy::default(),
        &PackagePolicy::default(),
        BatchPublication::new(
            |content: &Path| {
                let build = fixture
                    .releases
                    .begin_build(batch_id())
                    .map_err(|_| PublicationError::new())?;
                let output = fixture
                    .quartz
                    .build(content, build)
                    .map_err(|_| PublicationError::new())?;
                built.replace(Some(output));
                Ok(())
            },
            |_: &Path, commit: &str| {
                let output = built.take().ok_or_else(PublicationError::new)?;
                fixture
                    .releases
                    .prepare(output, commit, created_at())
                    .map_err(|_| PublicationError::new())?;
                Err(PublicationError::new())
            },
        ),
    );
    assert!(matches!(result, Err(GitTransactionError::TrialBuildFailed)));
    assert!(
        fixture
            .root
            .path()
            .join(format!("queue/{}/{}", QueueState::Processing, REQUEST_ID))
            .is_dir()
    );

    let processor = fixture.processor(repository);
    let outcome = processor
        .recover(&mut worker, batch_id(), created_at())
        .unwrap_or_else(|error| panic!("interrupted batch must recover: {error}"));
    let BatchCommitOutcome::Committed { commit, .. } = outcome else {
        panic!("recovered create request must retain its commit");
    };
    assert_eq!(
        fixture
            .releases
            .active_release()
            .unwrap_or_else(|error| panic!("active release must validate: {error}"))
            .unwrap_or_else(|| panic!("release must be active"))
            .commit(),
        commit
    );
    assert!(
        fixture
            .root
            .path()
            .join(format!("queue/{}/{}", QueueState::Completed, REQUEST_ID))
            .is_dir()
    );
}

#[test]
fn trial_failure_cleans_transaction_artifacts_for_an_exact_retry() {
    let fixture = Fixture::create();
    let repository = fixture.repository();
    let (queue, mut worker) = fixture.queue_and_worker();
    let claim = enqueue_and_claim(&queue, &mut worker);
    let stale = fixture
        .releases
        .begin_build(batch_id())
        .unwrap_or_else(|error| panic!("stale staging fixture must begin: {error}"));
    drop(stale);
    let processor = fixture.processor(repository);

    assert!(
        processor
            .process(
                &mut worker,
                batch_id(),
                std::slice::from_ref(&claim),
                created_at(),
            )
            .is_err()
    );
    assert_eq!(
        fs::read_dir(fixture.work_path.join("transactions"))
            .unwrap_or_else(|error| panic!("transactions must be readable: {error}"))
            .count(),
        0
    );
    let outcome = processor
        .process(&mut worker, batch_id(), &[claim], created_at())
        .unwrap_or_else(|error| panic!("exact batch retry must publish: {error}"));
    assert!(matches!(outcome, BatchCommitOutcome::Committed { .. }));
}

#[test]
fn repository_failure_after_trial_discards_unconsumed_build() {
    let fixture = Fixture::create_with_quartz_script(
        b"content=\noutput=\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = \"-d\" ]; then content=$2; shift 2; elif [ \"$1\" = \"-o\" ]; then output=$2; shift 2; else shift; fi\ndone\nprintf '%s\\n' '<p>fictional site</p>' > \"$output/index.html\"\nprintf '%s\\n' 'unexpected mutation' > \"$content/untracked.txt\"\n",
    );
    let repository = fixture.repository();
    let (queue, mut worker) = fixture.queue_and_worker();
    let claim = enqueue_and_claim(&queue, &mut worker);
    let processor = fixture.processor(repository);

    assert!(
        processor
            .process(
                &mut worker,
                batch_id(),
                std::slice::from_ref(&claim),
                created_at(),
            )
            .is_err()
    );
    assert!(
        !fixture
            .root
            .path()
            .join(format!("releases/.staging/{BATCH_ID}"))
            .exists()
    );
    assert_eq!(
        fs::read_dir(fixture.work_path.join("transactions"))
            .unwrap_or_else(|error| panic!("transactions must be readable: {error}"))
            .count(),
        0
    );
}

#[test]
fn all_failed_batch_reconciles_without_creating_a_release() {
    let fixture = Fixture::create();
    let repository = fixture.repository();
    let (queue, mut worker) = fixture.queue_and_worker();
    let claim = enqueue_missing_update(&queue, &mut worker);
    let processor = fixture.processor(repository);

    let outcome = processor
        .process(&mut worker, batch_id(), &[claim], created_at())
        .unwrap_or_else(|error| panic!("failed batch must finalize: {error}"));
    assert!(matches!(
        outcome,
        BatchCommitOutcome::NoChanges { failures }
            if failures.len() == 1
                && failures[0].error_code() == ErrorCode::DocumentNotFound
    ));
    assert!(
        fixture
            .releases
            .active_release()
            .unwrap_or_else(|error| panic!("empty release store must validate: {error}"))
            .is_none()
    );
    assert!(
        fixture
            .root
            .path()
            .join(format!("queue/{}/{}", QueueState::Failed, REQUEST_ID))
            .is_dir()
    );
    assert_eq!(
        fs::read_dir(fixture.work_path.join("transactions"))
            .unwrap_or_else(|error| panic!("transactions must be readable: {error}"))
            .count(),
        0
    );
}

#[test]
fn runtime_starts_clean_and_reports_an_empty_snapshot() {
    let fixture = Fixture::create();
    let repository = fixture.repository();
    let queue = FileQueue::initialize(fixture.root.path().join("queue"), PackagePolicy::default())
        .unwrap_or_else(|error| panic!("queue must initialize: {error}"));

    let (mut runtime, startup) = WorkerRuntime::start(
        &queue,
        fixture.processor(repository),
        Default::default(),
        created_at(),
    )
    .unwrap_or_else(|error| panic!("runtime must start: {error}"));
    assert_eq!(startup, StartupOutcome::Clean);
    assert_eq!(
        runtime
            .run_once(created_at())
            .unwrap_or_else(|error| panic!("empty cycle must complete: {error}")),
        WorkerRunOutcome::Idle
    );
}

#[test]
fn runtime_requeues_pretransaction_claims_then_processes_them() {
    let fixture = Fixture::create();
    let repository = fixture.repository();
    let (queue, mut worker) = fixture.queue_and_worker();
    let claim = enqueue_and_claim(&queue, &mut worker);
    let interrupted_batch = claim.token().batch_id();
    drop(worker);

    let (mut runtime, startup) = WorkerRuntime::start(
        &queue,
        fixture.processor(repository),
        Default::default(),
        created_at(),
    )
    .unwrap_or_else(|error| panic!("runtime must requeue the interrupted claim: {error}"));
    assert_eq!(
        startup,
        StartupOutcome::Requeued {
            batch_id: interrupted_batch,
            requests: 1,
        }
    );
    assert!(
        fixture
            .root
            .path()
            .join(format!("queue/{}/{REQUEST_ID}", QueueState::Pending))
            .is_dir()
    );

    let outcome = runtime
        .run_once(created_at())
        .unwrap_or_else(|error| panic!("requeued request must publish: {error}"));
    assert!(matches!(
        outcome,
        WorkerRunOutcome::Processed {
            outcome: BatchCommitOutcome::Committed { .. },
            ..
        }
    ));
}

#[test]
fn runtime_resumes_a_terminal_repository_transaction() {
    let fixture = Fixture::create();
    let repository = fixture.repository();
    let (queue, mut worker) = fixture.queue_and_worker();
    let claim = enqueue_and_claim(&queue, &mut worker);
    let built = RefCell::new(None);
    let result = repository.apply_batch_with_publication(
        &mut worker,
        batch_id(),
        std::slice::from_ref(&claim),
        ContentPolicy::default(),
        &PackagePolicy::default(),
        BatchPublication::new(
            |content: &Path| {
                let build = fixture
                    .releases
                    .begin_build(batch_id())
                    .map_err(|_| PublicationError::new())?;
                let output = fixture
                    .quartz
                    .build(content, build)
                    .map_err(|_| PublicationError::new())?;
                built.replace(Some(output));
                Ok(())
            },
            |_: &Path, commit: &str| {
                let output = built.take().ok_or_else(PublicationError::new)?;
                fixture
                    .releases
                    .prepare(output, commit, created_at())
                    .map_err(|_| PublicationError::new())?;
                Err(PublicationError::new())
            },
        ),
    );
    assert!(matches!(result, Err(GitTransactionError::TrialBuildFailed)));
    drop(worker);

    let (_runtime, startup) = WorkerRuntime::start(
        &queue,
        fixture.processor(repository),
        Default::default(),
        created_at(),
    )
    .unwrap_or_else(|error| panic!("runtime must resume publication: {error}"));
    assert!(matches!(
        startup,
        StartupOutcome::Resumed {
            batch_id: resumed_batch,
            outcome: BatchCommitOutcome::Committed { .. },
        } if resumed_batch == batch_id()
    ));
    assert!(
        fixture
            .root
            .path()
            .join(format!("queue/{}/{REQUEST_ID}", QueueState::Completed))
            .is_dir()
    );
}

#[test]
fn runtime_replays_a_preparing_repository_transaction() {
    let fixture = Fixture::create();
    let repository = fixture.repository();
    let (queue, mut worker) = fixture.queue_and_worker();
    let claim = enqueue_and_claim(&queue, &mut worker);
    let queue_identity = worker
        .queue_identity()
        .unwrap_or_else(|error| panic!("queue identity must remain available: {error}"));
    let output = Command::new("git")
        .arg(format!("--git-dir={}", fixture.repository_path.display()))
        .args(["rev-parse", "main"])
        .output()
        .unwrap_or_else(|error| panic!("base commit command must run: {error}"));
    assert!(output.status.success());
    let base_commit = String::from_utf8(output.stdout)
        .unwrap_or_else(|error| panic!("base commit must be UTF-8: {error}"));
    let acceptance_sequence = claim
        .package()
        .acceptance()
        .unwrap_or_else(|| panic!("claimed package must retain acceptance metadata"))
        .sequence;
    let journal = format!(
        "{{\n\
         \"schema_version\": 2,\n\
         \"batch_id\": \"{BATCH_ID}\",\n\
         \"queue_identity\": \"{queue_identity}\",\n\
         \"base_commit\": \"{}\",\n\
         \"claims\": [{{\n\
           \"request_id\": \"{REQUEST_ID}\",\n\
           \"attempt\": {},\n\
           \"acceptance_sequence\": {acceptance_sequence}\n\
         }}],\n\
         \"state\": {{\"phase\": \"preparing\"}}\n\
         }}\n",
        base_commit.trim(),
        claim.token().attempt()
    );
    fs::write(
        fixture
            .work_path
            .join(format!("transactions/{BATCH_ID}.json")),
        journal,
    )
    .unwrap_or_else(|error| panic!("preparing journal fixture must be written: {error}"));
    drop(worker);

    let (_runtime, startup) = WorkerRuntime::start(
        &queue,
        fixture.processor(repository),
        Default::default(),
        created_at(),
    )
    .unwrap_or_else(|error| panic!("runtime must replay preparation: {error}"));
    assert!(matches!(
        startup,
        StartupOutcome::Resumed {
            batch_id: resumed_batch,
            outcome: BatchCommitOutcome::Committed { .. },
        } if resumed_batch == batch_id()
    ));
    assert!(
        fixture
            .root
            .path()
            .join(format!("queue/{}/{REQUEST_ID}", QueueState::Completed))
            .is_dir()
    );
}

#[test]
fn runtime_requires_restart_after_a_failed_cycle() {
    let fixture = Fixture::create_with_quartz_script(b"exit 1\n");
    let repository = fixture.repository();
    let queue = FileQueue::initialize(fixture.root.path().join("queue"), PackagePolicy::default())
        .unwrap_or_else(|error| panic!("queue must initialize: {error}"));
    enqueue_create(&queue);
    let (mut runtime, startup) = WorkerRuntime::start(
        &queue,
        fixture.processor(repository),
        Default::default(),
        created_at(),
    )
    .unwrap_or_else(|error| panic!("runtime must start: {error}"));
    assert_eq!(startup, StartupOutcome::Clean);

    assert!(runtime.run_once(created_at()).is_err());
    assert!(matches!(
        runtime.run_once(created_at()),
        Err(WorkerRunError::RecoveryRequired)
    ));
}

#[test]
fn recovery_limit_cannot_be_smaller_than_a_new_batch() {
    let limits = WorkerRunLimits::new(
        NonZeroUsize::new(8).unwrap_or(NonZeroUsize::MIN),
        NonZeroUsize::new(100).unwrap_or(NonZeroUsize::MIN),
        NonZeroUsize::new(50).unwrap_or(NonZeroUsize::MIN),
    );

    assert_eq!(limits.maximum_scan_entries().get(), 8);
    assert_eq!(limits.maximum_requests().get(), 100);
    assert_eq!(limits.maximum_recovery_requests().get(), 100);
}

#[test]
fn separate_recovery_limit_allows_an_older_larger_batch() {
    let fixture = Fixture::create();
    let repository = fixture.repository();
    let (queue, mut worker) = fixture.queue_and_worker();
    let _first = enqueue_and_claim(&queue, &mut worker);
    enqueue_create_with_ids(&queue, SECOND_REQUEST_ID, SECOND_DOCUMENT_ID);
    let second_request_id = SECOND_REQUEST_ID
        .parse()
        .unwrap_or_else(|error| panic!("second request ID must parse: {error}"));
    worker
        .claim(second_request_id, batch_id())
        .unwrap_or_else(|error| panic!("second package must be claimed: {error}"));
    drop(worker);

    let one = NonZeroUsize::MIN;
    let scan = NonZeroUsize::new(16).unwrap_or(NonZeroUsize::MIN);
    let too_small = WorkerRunLimits::new(scan, one, one);
    assert!(matches!(
        WorkerRuntime::start(
            &queue,
            fixture.processor(repository.clone()),
            too_small,
            created_at(),
        ),
        Err(WorkerRunError::TooManyProcessingClaims { maximum: 1 })
    ));

    let two = NonZeroUsize::new(2).unwrap_or(NonZeroUsize::MIN);
    let recoverable = WorkerRunLimits::new(scan, one, two);
    let (_runtime, startup) = WorkerRuntime::start(
        &queue,
        fixture.processor(repository),
        recoverable,
        created_at(),
    )
    .unwrap_or_else(|error| panic!("larger recovery bound must accept old claims: {error}"));
    assert!(matches!(
        startup,
        StartupOutcome::Requeued { requests: 2, .. }
    ));
}

#[test]
fn scheduler_waits_for_debounce_before_processing() {
    let fixture = Fixture::create();
    let repository = fixture.repository();
    let queue = FileQueue::initialize(fixture.root.path().join("queue"), PackagePolicy::default())
        .unwrap_or_else(|error| panic!("queue must initialize: {error}"));
    enqueue_create(&queue);
    let (mut runtime, startup) = WorkerRuntime::start(
        &queue,
        fixture.processor(repository),
        Default::default(),
        created_at(),
    )
    .unwrap_or_else(|error| panic!("runtime must start: {error}"));
    assert_eq!(startup, StartupOutcome::Clean);

    assert!(matches!(
        runtime.poll_once(BatchSchedule::default(), OffsetDateTime::now_utc()),
        Ok(WorkerPollOutcome::Waiting { .. })
    ));
    let outcome = runtime
        .poll_once(
            BatchSchedule::default(),
            OffsetDateTime::now_utc() + TimeDuration::minutes(1),
        )
        .unwrap_or_else(|error| panic!("debounced batch must process: {error}"));
    assert!(matches!(
        outcome,
        WorkerPollOutcome::Processed {
            outcome: BatchCommitOutcome::Committed { .. },
            ..
        }
    ));
}

#[test]
fn scheduler_leaves_arrivals_after_observation_for_the_next_batch() {
    let fixture = Fixture::create();
    let repository = fixture.repository();
    let queue = FileQueue::initialize(fixture.root.path().join("queue"), PackagePolicy::default())
        .unwrap_or_else(|error| panic!("queue must initialize: {error}"));
    enqueue_create(&queue);
    enqueue_create_with_ids(&queue, SECOND_REQUEST_ID, SECOND_DOCUMENT_ID);
    let one = NonZeroUsize::MIN;
    let one_hundred = NonZeroUsize::new(100).unwrap_or(NonZeroUsize::MIN);
    let limits = WorkerRunLimits::new(one, one_hundred, one_hundred);
    let (mut runtime, startup) =
        WorkerRuntime::start(&queue, fixture.processor(repository), limits, created_at())
            .unwrap_or_else(|error| panic!("runtime must start: {error}"));
    assert_eq!(startup, StartupOutcome::Clean);
    let ready_time = OffsetDateTime::now_utc() + TimeDuration::minutes(10);
    assert!(matches!(
        runtime.poll_once(BatchSchedule::default(), ready_time),
        Ok(WorkerPollOutcome::Scanning {
            scanned_entries: 1,
            ..
        })
    ));

    enqueue_create_with_ids(&queue, THIRD_REQUEST_ID, THIRD_DOCUMENT_ID);
    let outcome = loop {
        match runtime.poll_once(BatchSchedule::default(), ready_time) {
            Ok(WorkerPollOutcome::Scanning { .. }) => {}
            Ok(outcome) => break outcome,
            Err(error) => panic!("fixed snapshot must process: {error}"),
        }
    };
    assert!(matches!(
        outcome,
        WorkerPollOutcome::Processed {
            outcome: BatchCommitOutcome::Committed { .. },
            ..
        }
    ));
    assert!(
        fixture
            .root
            .path()
            .join(format!("queue/{}/{THIRD_REQUEST_ID}", QueueState::Pending))
            .is_dir()
    );
}

#[test]
fn scheduler_reports_invalid_only_snapshots_without_a_commit() {
    let fixture = Fixture::create();
    let repository = fixture.repository();
    let queue = FileQueue::initialize(fixture.root.path().join("queue"), PackagePolicy::default())
        .unwrap_or_else(|error| panic!("queue must initialize: {error}"));
    enqueue_create(&queue);
    fs::write(
        fixture
            .root
            .path()
            .join(format!("queue/pending/{REQUEST_ID}/acceptance.json")),
        b"invalid acceptance metadata\n",
    )
    .unwrap_or_else(|error| panic!("acceptance fixture must be corrupted: {error}"));
    let (mut runtime, startup) = WorkerRuntime::start(
        &queue,
        fixture.processor(repository),
        Default::default(),
        created_at(),
    )
    .unwrap_or_else(|error| panic!("runtime must start: {error}"));
    assert_eq!(startup, StartupOutcome::Clean);

    assert_eq!(
        runtime
            .poll_once(BatchSchedule::default(), OffsetDateTime::now_utc())
            .unwrap_or_else(|error| panic!("invalid snapshot must close: {error}")),
        WorkerPollOutcome::ClosedWithoutCommit {
            reason: BatchCloseReason::InvalidAcceptance,
        }
    );
    assert!(
        fixture
            .root
            .path()
            .join(format!("queue/{}/{REQUEST_ID}", QueueState::Failed))
            .is_dir()
    );
}

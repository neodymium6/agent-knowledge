use std::cell::RefCell;
use std::ffi::OsString;
use std::fs;
use std::io::Cursor;
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
use time::OffsetDateTime;

use super::BatchProcessor;

const REQUEST_ID: &str = "01K00000000000000000000001";
const DOCUMENT_ID: &str = "01K00000000000000000000002";
const BATCH_ID: &str = "01K00000000000000000000003";
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
    let request = format!(
        "{{\n\
         \"protocol_version\": 1,\n\
         \"request_id\": \"{REQUEST_ID}\",\n\
         \"title\": \"Create a fictional experiment\",\n\
         \"project\": \"fictional-project\",\n\
         \"document_type\": \"experiment\",\n\
         \"created_at\": \"2026-07-31T04:00:00Z\",\n\
         \"operations\": [{{\n\
           \"type\": \"create_document\",\n\
           \"document_id\": \"{DOCUMENT_ID}\",\n\
           \"content\": \"index.md\"\n\
         }}]\n\
         }}\n"
    );
    let markdown = format!(
        "---\n\
         schema_version: 1\n\
         document_id: {DOCUMENT_ID}\n\
         title: Fictional experiment\n\
         created: 2026-07-31T03:50:00Z\n\
         request_id: {REQUEST_ID}\n\
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
    worker
        .claim(request_id(), batch_id())
        .unwrap_or_else(|error| panic!("package must be claimed: {error}"))
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

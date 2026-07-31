use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::process::Command;

use agent_knowledge_core::{BatchId, ErrorCode, PayloadPath, RequestId, Revision};
use agent_knowledge_queue::{
    ClaimedPackage, FileQueue, PackagePolicy, ProcessingScanOutcome, WorkerSession,
};
use ulid::Ulid;

use super::{
    BatchCommitOutcome, GitIdentity, GitRepository, GitTransactionError, parse_text, staged_stats,
};
use crate::ContentPolicy;
use crate::apply::AppliedFileMove;

const FIRST_REQUEST_ID: &str = "01K00000000000000000000001";
const SECOND_REQUEST_ID: &str = "01K00000000000000000000002";
const DOCUMENT_ID: &str = "01K00000000000000000000003";
const BATCH_ID: &str = "01K00000000000000000000004";
const SECOND_BATCH_ID: &str = "01K00000000000000000000005";

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "agent-knowledge-git-transaction-test-{}",
            Ulid::generate()
        ));
        if let Err(error) = fs::create_dir(&path) {
            panic!("test directory must be created: {error}");
        }
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.path) {
            eprintln!("failed to remove test directory: {error}");
        }
    }
}

struct GitFixture {
    repository: PathBuf,
    canonical: PathBuf,
    work: PathBuf,
}

impl GitFixture {
    fn initialize(root: &Path) -> Self {
        let repository = root.join("repository");
        let seed = root.join("seed");
        let canonical = root.join("content");
        let work = root.join("work");

        git(
            None,
            ["init", "--bare", "--initial-branch=main"],
            Some(&repository),
        );
        git(None, ["init", "--initial-branch=main"], Some(&seed));
        git(
            Some(&seed),
            ["config", "user.name", "Agent Knowledge Test"],
            None,
        );
        git(
            Some(&seed),
            ["config", "user.email", "agent-knowledge@example.invalid"],
            None,
        );
        git(
            Some(&seed),
            [
                "commit",
                "--allow-empty",
                "-m",
                "Initialize fictional knowledge",
            ],
            None,
        );
        git(Some(&seed), ["remote", "add", "origin"], Some(&repository));
        git(Some(&seed), ["push", "origin", "main"], None);
        let status = Command::new("git")
            .arg(format!("--git-dir={}", repository.display()))
            .args(["worktree", "add"])
            .arg(&canonical)
            .arg("main")
            .status();
        match status {
            Ok(status) if status.success() => {}
            Ok(status) => panic!("canonical worktree creation failed with {status}"),
            Err(error) => panic!("canonical worktree command failed: {error}"),
        }
        if let Err(error) = fs::create_dir(&work) {
            panic!("work root must be created: {error}");
        }

        Self {
            repository,
            canonical,
            work,
        }
    }

    fn open(&self) -> GitRepository {
        let identity =
            match GitIdentity::new("Agent Knowledge Worker", "agent-knowledge@example.invalid") {
                Ok(identity) => identity,
                Err(error) => panic!("fictional identity must be valid: {error}"),
            };
        match GitRepository::open(
            &self.repository,
            &self.canonical,
            &self.work,
            "main",
            identity,
        ) {
            Ok(repository) => repository,
            Err(error) => panic!("Git fixture must open: {error}"),
        }
    }

    fn official_commit(&self) -> String {
        git_output(
            None,
            [
                format!("--git-dir={}", self.repository.display()),
                "rev-parse".into(),
                "refs/heads/main".into(),
            ],
        )
    }

    fn transaction_count(&self) -> usize {
        directory_entry_count(&self.work.join("transactions"))
    }

    fn worktree_count(&self) -> usize {
        directory_entry_count(&self.work.join("worktrees"))
    }
}

fn directory_entry_count(path: &Path) -> usize {
    fs::read_dir(path)
        .map(|entries| entries.count())
        .unwrap_or(usize::MAX)
}

fn git<const N: usize>(
    working_directory: Option<&Path>,
    arguments: [&str; N],
    path_argument: Option<&Path>,
) {
    let mut command = Command::new("git");
    if let Some(working_directory) = working_directory {
        command.current_dir(working_directory);
    }
    command.args(arguments);
    if let Some(path_argument) = path_argument {
        command.arg(path_argument);
    }
    let status = command.status();
    match status {
        Ok(status) if status.success() => {}
        Ok(status) => panic!("Git fixture command failed with {status}"),
        Err(error) => panic!("Git fixture command failed: {error}"),
    }
}

fn git_output<const N: usize>(working_directory: Option<&Path>, arguments: [String; N]) -> String {
    let mut command = Command::new("git");
    if let Some(working_directory) = working_directory {
        command.current_dir(working_directory);
    }
    let output = match command.args(arguments).output() {
        Ok(output) => output,
        Err(error) => panic!("Git fixture command failed: {error}"),
    };
    if !output.status.success() {
        panic!(
            "Git fixture command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    match std::str::from_utf8(&output.stdout) {
        Ok(value) => value.trim().into(),
        Err(error) => panic!("Git fixture output must be UTF-8: {error}"),
    }
}

fn parse_request_id(value: &str) -> RequestId {
    match value.parse() {
        Ok(value) => value,
        Err(error) => panic!("fixture request ID must parse: {error}"),
    }
}

fn parse_batch_id() -> BatchId {
    parse_batch_id_value(BATCH_ID)
}

fn parse_batch_id_value(value: &str) -> BatchId {
    match value.parse() {
        Ok(value) => value,
        Err(error) => panic!("fixture batch ID must parse: {error}"),
    }
}

fn markdown(request_id: &str, title: &str) -> String {
    format!(
        "---\n\
         schema_version: 1\n\
         document_id: {DOCUMENT_ID}\n\
         title: {title}\n\
         created: 2026-07-31T03:50:00Z\n\
         request_id: {request_id}\n\
         status: active\n\
         ---\n\
         Fictional transaction body.\n"
    )
}

fn create_request(request_id: &str, title: &str) -> String {
    format!(
        "{{\n\
         \"protocol_version\": 1,\n\
         \"request_id\": \"{request_id}\",\n\
         \"title\": \"{title}\",\n\
         \"project\": \"fictional-project\",\n\
         \"document_type\": \"experiment\",\n\
         \"created_at\": \"2026-07-31T04:00:00Z\",\n\
         \"operations\": [{{\n\
           \"type\": \"create_document\",\n\
           \"document_id\": \"{DOCUMENT_ID}\",\n\
           \"content\": \"index.md\"\n\
         }}]\n\
         }}\n"
    )
}

fn missing_update_request() -> (String, String) {
    let revision = Revision::from_bytes([7; 32]);
    let markdown = format!(
        "---\n\
         schema_version: 1\n\
         document_id: {DOCUMENT_ID}\n\
         title: Missing fictional runbook\n\
         created: 2026-07-31T03:50:00Z\n\
         updated: 2026-07-31T04:00:00Z\n\
         request_id: {FIRST_REQUEST_ID}\n\
         status: active\n\
         ---\n\
         Fictional update.\n"
    );
    let request = format!(
        "{{\n\
         \"protocol_version\": 1,\n\
         \"request_id\": \"{FIRST_REQUEST_ID}\",\n\
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
    (request, markdown)
}

fn enqueue_and_claim(
    queue: &FileQueue,
    worker: &mut WorkerSession,
    request_id: &str,
    request: &str,
    markdown: &str,
) -> ClaimedPackage {
    enqueue_and_claim_for_batch(
        queue,
        worker,
        request_id,
        request,
        markdown,
        parse_batch_id(),
    )
}

fn enqueue_and_claim_for_batch(
    queue: &FileQueue,
    worker: &mut WorkerSession,
    request_id: &str,
    request: &str,
    markdown: &str,
    batch_id: BatchId,
) -> ClaimedPackage {
    let mut incoming = match queue.begin() {
        Ok(incoming) => incoming,
        Err(error) => panic!("incoming package must begin: {error}"),
    };
    if let Err(error) = incoming.write_request(&mut Cursor::new(request.as_bytes())) {
        panic!("request must be staged: {error}");
    }
    let payload_path = match "index.md".parse::<PayloadPath>() {
        Ok(path) => path,
        Err(error) => panic!("payload path must parse: {error}"),
    };
    if let Err(error) = incoming.add_payload(payload_path, &mut Cursor::new(markdown.as_bytes())) {
        panic!("payload must be staged: {error}");
    }
    if let Err(error) = incoming.accept() {
        panic!("package must be accepted: {error}");
    }
    match worker.claim(parse_request_id(request_id), batch_id) {
        Ok(claim) => claim,
        Err(error) => panic!("package must be claimed: {error}"),
    }
}

fn queue_and_worker(root: &Path) -> (FileQueue, WorkerSession) {
    let queue = match FileQueue::initialize(root, PackagePolicy::default()) {
        Ok(queue) => queue,
        Err(error) => panic!("queue fixture must initialize: {error}"),
    };
    let mut worker = match queue.try_worker_session() {
        Ok(worker) => worker,
        Err(error) => panic!("worker fixture must start: {error}"),
    };
    match worker.scan_processing(16) {
        Ok(ProcessingScanOutcome::Complete { claims, .. }) if claims.is_empty() => {}
        Ok(_) => panic!("new queue recovery must complete without claims"),
        Err(error) => panic!("new queue recovery must succeed: {error}"),
    }
    (queue, worker)
}

#[test]
fn commits_successes_and_isolates_a_conflicting_request() {
    let root = TestDirectory::new();
    let git = GitFixture::initialize(root.path());
    let base = git.official_commit();
    let (queue, mut worker) = queue_and_worker(&root.path().join("queue"));
    let first = enqueue_and_claim(
        &queue,
        &mut worker,
        FIRST_REQUEST_ID,
        &create_request(FIRST_REQUEST_ID, "Create a fictional experiment"),
        &markdown(FIRST_REQUEST_ID, "Fictional experiment"),
    );
    let second = enqueue_and_claim(
        &queue,
        &mut worker,
        SECOND_REQUEST_ID,
        &create_request(SECOND_REQUEST_ID, "Create a conflicting experiment"),
        &markdown(SECOND_REQUEST_ID, "Conflicting experiment"),
    );

    let repository = git.open();
    let outcome = match repository.apply_batch(
        &mut worker,
        parse_batch_id(),
        &[first, second],
        ContentPolicy::default(),
        &PackagePolicy::default(),
    ) {
        Ok(outcome) => outcome,
        Err(error) => panic!("mixed batch must commit healthy requests: {error}"),
    };
    let BatchCommitOutcome::Committed {
        commit,
        successful,
        failures,
    } = outcome
    else {
        panic!("one healthy request must produce a commit");
    };
    assert_ne!(commit, base);
    assert_eq!(successful.len(), 1);
    assert_eq!(
        successful[0].request_id(),
        parse_request_id(FIRST_REQUEST_ID)
    );
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].error_code(), ErrorCode::DocumentIdConflict);
    assert_eq!(
        failures[0].token().request_id(),
        parse_request_id(SECOND_REQUEST_ID)
    );
    assert!(
        git.canonical
            .join(format!(
                "projects/fictional-project/experiments/2026-07-31-{DOCUMENT_ID}/index.md"
            ))
            .is_file()
    );
    let message = git_output(
        None,
        [
            format!("--git-dir={}", git.repository.display()),
            "show".into(),
            "-s".into(),
            "--format=%B".into(),
            commit.clone(),
        ],
    );
    assert!(message.contains("knowledge snapshot: 1 changes"));
    assert!(message.contains(&format!("Knowledge-Request: {FIRST_REQUEST_ID}")));
    assert!(!message.contains(&format!("Knowledge-Request: {SECOND_REQUEST_ID}")));
    assert_eq!(git.worktree_count(), 0);
    assert_eq!(git.transaction_count(), 1);
    assert!(matches!(
        repository.reconcile_batch(&mut worker, parse_batch_id()),
        Ok(BatchCommitOutcome::Committed {
            commit: reconciled,
            ..
        }) if reconciled == commit
    ));
    assert_eq!(git.transaction_count(), 0);
    let repeated = worker.reconcile_batch(
        parse_batch_id(),
        &successful,
        &[(failures[0].token(), failures[0].error_code())],
    );
    if let Err(error) = repeated {
        panic!("terminal queue reconciliation must be idempotent: {error}");
    }
    assert!(
        root.path()
            .join(format!("queue/completed/{FIRST_REQUEST_ID}"))
            .is_dir()
    );
    assert!(
        root.path()
            .join(format!("queue/failed/{SECOND_REQUEST_ID}/result.json"))
            .is_file()
    );
}

#[test]
fn all_request_failures_leave_the_official_commit_unchanged() {
    let root = TestDirectory::new();
    let git = GitFixture::initialize(root.path());
    let base = git.official_commit();
    let (queue, mut worker) = queue_and_worker(&root.path().join("queue"));
    let (request, payload) = missing_update_request();
    let claim = enqueue_and_claim(&queue, &mut worker, FIRST_REQUEST_ID, &request, &payload);
    let outcome = match git.open().apply_batch(
        &mut worker,
        parse_batch_id(),
        std::slice::from_ref(&claim),
        ContentPolicy::default(),
        &PackagePolicy::default(),
    ) {
        Ok(outcome) => outcome,
        Err(error) => panic!("missing document must be isolated: {error}"),
    };
    assert!(matches!(
        outcome,
        BatchCommitOutcome::NoChanges { failures }
            if failures.len() == 1
                && failures[0].error_code() == ErrorCode::DocumentNotFound
    ));
    assert_eq!(git.official_commit(), base);
    assert_eq!(git.worktree_count(), 0);
    assert_eq!(git.transaction_count(), 1);
    let repository = git.open();
    assert!(matches!(
        repository.recover_batch(&worker, parse_batch_id()),
        Ok(BatchCommitOutcome::NoChanges { failures })
            if failures.len() == 1
                && failures[0].error_code() == ErrorCode::DocumentNotFound
    ));
    assert!(matches!(
        repository.reconcile_batch(&mut worker, parse_batch_id()),
        Ok(BatchCommitOutcome::NoChanges { failures })
            if failures.len() == 1
                && failures[0].error_code() == ErrorCode::DocumentNotFound
    ));
    assert_eq!(git.transaction_count(), 0);
}

#[test]
fn rejects_a_request_timestamp_after_durable_acceptance() {
    let root = TestDirectory::new();
    let git = GitFixture::initialize(root.path());
    let (queue, mut worker) = queue_and_worker(&root.path().join("queue"));
    let future_request = create_request(FIRST_REQUEST_ID, "Create a future fictional experiment")
        .replace(
            "\"created_at\": \"2026-07-31T04:00:00Z\"",
            "\"created_at\": \"9999-07-31T04:00:00Z\"",
        );
    let claim = enqueue_and_claim(
        &queue,
        &mut worker,
        FIRST_REQUEST_ID,
        &future_request,
        &markdown(FIRST_REQUEST_ID, "Future fictional experiment"),
    );
    assert!(matches!(
        git.open().apply_batch(
            &mut worker,
            parse_batch_id(),
            &[claim],
            ContentPolicy::default(),
            &PackagePolicy::default(),
        ),
        Ok(BatchCommitOutcome::NoChanges { failures })
            if failures.len() == 1
                && failures[0].error_code() == ErrorCode::InvalidRequest
    ));
}

#[test]
fn resumes_publication_from_a_committed_journal_after_interruption() {
    let root = TestDirectory::new();
    let git = GitFixture::initialize(root.path());
    let base = git.official_commit();
    let (queue, mut worker) = queue_and_worker(&root.path().join("queue"));
    let claim = enqueue_and_claim(
        &queue,
        &mut worker,
        FIRST_REQUEST_ID,
        &create_request(
            FIRST_REQUEST_ID,
            "Create a recoverable fictional experiment",
        ),
        &markdown(FIRST_REQUEST_ID, "Recoverable fictional experiment"),
    );
    let repository = git.open();
    let interrupted = repository.apply_batch_with_hook(
        &mut worker,
        parse_batch_id(),
        std::slice::from_ref(&claim),
        ContentPolicy::default(),
        &PackagePolicy::default(),
        |_, _| Err(GitTransactionError::InvalidGitOutput),
    );
    assert!(matches!(
        interrupted,
        Err(GitTransactionError::InvalidGitOutput)
    ));
    assert_eq!(git.official_commit(), base);
    assert_eq!(git.transaction_count(), 1);
    assert_eq!(git.worktree_count(), 1);
    let journal_path = git.work.join(format!("transactions/{BATCH_ID}.json"));
    let original_journal = match fs::read(&journal_path) {
        Ok(bytes) => bytes,
        Err(error) => panic!("transaction journal must be readable: {error}"),
    };
    let mut changed_journal: serde_json::Value = match serde_json::from_slice(&original_journal) {
        Ok(value) => value,
        Err(error) => panic!("transaction journal must decode: {error}"),
    };
    changed_journal["state"]["commit"] = serde_json::Value::String(base.clone());
    let changed_journal = match serde_json::to_vec(&changed_journal) {
        Ok(bytes) => bytes,
        Err(error) => panic!("changed journal fixture must encode: {error}"),
    };
    if let Err(error) = fs::write(&journal_path, changed_journal) {
        panic!("changed journal fixture must be written: {error}");
    }
    assert!(matches!(
        repository.recover_batch(&worker, parse_batch_id()),
        Err(GitTransactionError::JournalMismatch)
    ));
    if let Err(error) = fs::write(&journal_path, original_journal) {
        panic!("original journal fixture must be restored: {error}");
    }

    let (_other_queue, other_worker) =
        queue_and_worker(&root.path().join("unrelated-fictional-queue"));
    assert!(matches!(
        repository.recover_batch(&other_worker, parse_batch_id()),
        Err(GitTransactionError::JournalMismatch)
    ));

    let outcome = match repository.recover_batch(&worker, parse_batch_id()) {
        Ok(outcome) => outcome,
        Err(error) => panic!("committed journal must resume publication: {error}"),
    };
    let BatchCommitOutcome::Committed { commit, .. } = outcome else {
        panic!("resumed transaction must retain its commit outcome");
    };
    assert_ne!(commit, base);
    assert_eq!(git.official_commit(), commit);
    assert_eq!(git.worktree_count(), 0);
    assert_eq!(git.transaction_count(), 1);
    assert!(matches!(
        repository.reconcile_batch(&mut worker, parse_batch_id()),
        Ok(BatchCommitOutcome::Committed {
            commit: reconciled,
            ..
        }) if reconciled == commit
    ));
    assert_eq!(git.transaction_count(), 0);
}

#[test]
fn counts_an_explicitly_moved_and_rewritten_file_without_git_rename_detection() {
    let root = TestDirectory::new();
    let fixture = GitFixture::initialize(root.path());
    let source = PathBuf::from("projects/fictional-a/runbooks/source/index.md");
    let destination = PathBuf::from("projects/fictional-a/archive/runbooks/source/index.md");
    write_tracked_fixture(
        &fixture.canonical,
        &source,
        b"A compact fictional source document.\n",
    );
    let base = commit_canonical_fixture(&fixture.canonical, "Add fictional source document");

    let destination_path = fixture.canonical.join(&destination);
    if let Some(parent) = destination_path.parent()
        && let Err(error) = fs::create_dir_all(parent)
    {
        panic!("archive fixture parent must be created: {error}");
    }
    if let Err(error) = fs::rename(fixture.canonical.join(&source), &destination_path) {
        panic!("archive fixture must move: {error}");
    }
    if let Err(error) = fs::write(
        &destination_path,
        b"A completely rewritten fictional archived document with unrelated bytes.\n",
    ) {
        panic!("archive fixture must be rewritten: {error}");
    }
    git(Some(&fixture.canonical), ["add", "--all"], None);

    let stats = match staged_stats(
        &fixture.canonical,
        &base,
        &[AppliedFileMove {
            source,
            destination,
        }],
    ) {
        Ok(stats) => stats,
        Err(error) => panic!("explicit move must authorize the paired deletion: {error}"),
    };
    assert_eq!(stats.added, 0);
    assert_eq!(stats.modified, 1);
    assert_eq!(stats.deleted, 0);
}

fn write_tracked_fixture(worktree: &Path, relative_path: &Path, bytes: &[u8]) {
    let path = worktree.join(relative_path);
    if let Some(parent) = path.parent()
        && let Err(error) = fs::create_dir_all(parent)
    {
        panic!("tracked fixture parent must be created: {error}");
    }
    if let Err(error) = fs::write(path, bytes) {
        panic!("tracked fixture must be written: {error}");
    }
}

fn commit_canonical_fixture(worktree: &Path, message: &str) -> String {
    git(Some(worktree), ["add", "--all"], None);
    let status = Command::new("git")
        .current_dir(worktree)
        .args([
            "-c",
            "user.name=Agent Knowledge Test",
            "-c",
            "user.email=agent-knowledge@example.invalid",
            "commit",
            "-m",
            message,
        ])
        .status();
    match status {
        Ok(status) if status.success() => {}
        Ok(status) => panic!("canonical fixture commit failed with {status}"),
        Err(error) => panic!("canonical fixture commit command failed: {error}"),
    }
    git_output(Some(worktree), ["rev-parse".into(), "HEAD".into()])
}

#[test]
fn rejects_unordered_and_duplicate_batch_claims_before_writing() {
    let root = TestDirectory::new();
    let git = GitFixture::initialize(root.path());
    let (queue, mut worker) = queue_and_worker(&root.path().join("queue"));
    let first = enqueue_and_claim(
        &queue,
        &mut worker,
        FIRST_REQUEST_ID,
        &create_request(FIRST_REQUEST_ID, "Create the first fictional experiment"),
        &markdown(FIRST_REQUEST_ID, "First fictional experiment"),
    );
    let second = enqueue_and_claim(
        &queue,
        &mut worker,
        SECOND_REQUEST_ID,
        &create_request(SECOND_REQUEST_ID, "Create the second fictional experiment"),
        &markdown(SECOND_REQUEST_ID, "Second fictional experiment"),
    );
    let repository = git.open();
    assert!(matches!(
        repository.apply_batch(
            &mut worker,
            parse_batch_id(),
            &[second.clone(), first.clone()],
            ContentPolicy::default(),
            &PackagePolicy::default(),
        ),
        Err(GitTransactionError::InvalidClaims)
    ));
    assert!(matches!(
        repository.apply_batch(
            &mut worker,
            parse_batch_id(),
            &[first.clone(), first],
            ContentPolicy::default(),
            &PackagePolicy::default(),
        ),
        Err(GitTransactionError::InvalidClaims)
    ));
    assert_eq!(git.transaction_count(), 0);
    assert_eq!(git.worktree_count(), 0);
}

#[test]
fn blocks_a_new_batch_until_the_published_journal_is_finalized() {
    let root = TestDirectory::new();
    let git = GitFixture::initialize(root.path());
    let (queue, mut worker) = queue_and_worker(&root.path().join("queue"));
    let first = enqueue_and_claim(
        &queue,
        &mut worker,
        FIRST_REQUEST_ID,
        &create_request(FIRST_REQUEST_ID, "Create the first fictional experiment"),
        &markdown(FIRST_REQUEST_ID, "First fictional experiment"),
    );
    let repository = git.open();
    let first_outcome = match repository.apply_batch(
        &mut worker,
        parse_batch_id(),
        &[first],
        ContentPolicy::default(),
        &PackagePolicy::default(),
    ) {
        Ok(outcome) => outcome,
        Err(error) => panic!("first batch must publish: {error}"),
    };
    let BatchCommitOutcome::Committed {
        commit: first_commit,
        ..
    } = first_outcome
    else {
        panic!("first batch must create a commit");
    };

    let second_batch = parse_batch_id_value(SECOND_BATCH_ID);
    let second = enqueue_and_claim_for_batch(
        &queue,
        &mut worker,
        SECOND_REQUEST_ID,
        &create_request(SECOND_REQUEST_ID, "Create the second fictional experiment"),
        &markdown(SECOND_REQUEST_ID, "Second fictional experiment"),
        second_batch,
    );
    assert!(matches!(
        repository.apply_batch(
            &mut worker,
            second_batch,
            &[second],
            ContentPolicy::default(),
            &PackagePolicy::default(),
        ),
        Err(GitTransactionError::UnfinishedTransaction)
    ));
    assert!(matches!(
        repository.reconcile_batch(&mut worker, parse_batch_id()),
        Ok(BatchCommitOutcome::Committed { commit, .. }) if commit == first_commit
    ));
}

#[test]
fn refuses_to_start_with_a_dirty_canonical_worktree() {
    let root = TestDirectory::new();
    let git = GitFixture::initialize(root.path());
    let (queue, mut worker) = queue_and_worker(&root.path().join("queue"));
    let claim = enqueue_and_claim(
        &queue,
        &mut worker,
        FIRST_REQUEST_ID,
        &create_request(FIRST_REQUEST_ID, "Create a fictional experiment"),
        &markdown(FIRST_REQUEST_ID, "Fictional experiment"),
    );
    if let Err(error) = fs::write(git.canonical.join("untracked.json"), b"{}\n") {
        panic!("dirty canonical fixture must be written: {error}");
    }
    assert!(matches!(
        git.open().apply_batch(
            &mut worker,
            parse_batch_id(),
            std::slice::from_ref(&claim),
            ContentPolicy::default(),
            &PackagePolicy::default(),
        ),
        Err(GitTransactionError::CanonicalWorktreeDirty)
    ));
    assert_eq!(git.transaction_count(), 0);
    assert_eq!(git.worktree_count(), 0);

    if let Err(error) = fs::remove_file(git.canonical.join("untracked.json")) {
        panic!("untracked fixture must be removed: {error}");
    }
    if let Err(error) = fs::write(git.repository.join("info/exclude"), "ignored.json\n") {
        panic!("fictional ignore rule must be written: {error}");
    }
    if let Err(error) = fs::write(git.canonical.join("ignored.json"), b"{}\n") {
        panic!("ignored canonical fixture must be written: {error}");
    }
    assert!(matches!(
        git.open().apply_batch(
            &mut worker,
            parse_batch_id(),
            &[claim],
            ContentPolicy::default(),
            &PackagePolicy::default(),
        ),
        Err(GitTransactionError::CanonicalWorktreeDirty)
    ));
}

#[test]
fn compare_and_swap_refuses_a_concurrent_official_update() {
    let root = TestDirectory::new();
    let git = GitFixture::initialize(root.path());
    let base = git.official_commit();
    let (queue, mut worker) = queue_and_worker(&root.path().join("queue"));
    let claim = enqueue_and_claim(
        &queue,
        &mut worker,
        FIRST_REQUEST_ID,
        &create_request(FIRST_REQUEST_ID, "Create a fictional experiment"),
        &markdown(FIRST_REQUEST_ID, "Fictional experiment"),
    );
    let repository_path = git.repository.clone();
    let result = git.open().apply_batch_with_hook(
        &mut worker,
        parse_batch_id(),
        &[claim],
        ContentPolicy::default(),
        &PackagePolicy::default(),
        move |expected, _| {
            let tree = git_output(
                None,
                [
                    format!("--git-dir={}", repository_path.display()),
                    "rev-parse".into(),
                    format!("{expected}^{{tree}}"),
                ],
            );
            let output = Command::new("git")
                .arg(format!("--git-dir={}", repository_path.display()))
                .args([
                    "commit-tree",
                    &tree,
                    "-p",
                    expected,
                    "-m",
                    "Concurrent commit",
                ])
                .env("GIT_AUTHOR_NAME", "Concurrent Test")
                .env("GIT_AUTHOR_EMAIL", "concurrent@example.invalid")
                .env("GIT_COMMITTER_NAME", "Concurrent Test")
                .env("GIT_COMMITTER_EMAIL", "concurrent@example.invalid")
                .output()
                .map_err(GitTransactionError::Io)?;
            if !output.status.success() {
                return Err(GitTransactionError::InvalidGitOutput);
            }
            let competing = parse_text(&output.stdout)?;
            let status = Command::new("git")
                .arg(format!("--git-dir={}", repository_path.display()))
                .args(["update-ref", "refs/heads/main", competing, expected])
                .status()
                .map_err(GitTransactionError::Io)?;
            if !status.success() {
                return Err(GitTransactionError::InvalidGitOutput);
            }
            Ok(())
        },
    );
    assert!(matches!(
        result,
        Err(GitTransactionError::OfficialBranchChanged { expected, actual })
            if expected == base && actual != base
    ));
    assert_ne!(git.official_commit(), base);
    let base_tree = git_output(
        None,
        [
            format!("--git-dir={}", git.repository.display()),
            "rev-parse".into(),
            format!("{base}^{{tree}}"),
        ],
    );
    let canonical_index_tree = git_output(Some(&git.canonical), ["write-tree".into()]);
    assert_eq!(canonical_index_tree, base_tree);
}

#[test]
fn rejects_unsafe_commit_identity_values() {
    assert!(matches!(
        GitIdentity::new("Worker\nInjected", "worker@example.invalid"),
        Err(GitTransactionError::InvalidIdentity)
    ));
}

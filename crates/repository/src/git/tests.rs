use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::process::Command;

#[cfg(unix)]
use std::ffi::OsString;
#[cfg(unix)]
use std::os::unix::ffi::{OsStrExt, OsStringExt};

use agent_knowledge_core::{BatchId, ErrorCode, PayloadPath, RequestId, Revision};
use agent_knowledge_queue::{
    ClaimedPackage, FileQueue, PackagePolicy, ProcessingScanOutcome, WorkerSession,
};
use ulid::Ulid;

use super::{
    BatchCommitOutcome, GitIdentity, GitRepository, GitTransactionError, TransactionHooks,
    accept_trial_build, interrupt_publication, parse_git_version, parse_text, staged_stats,
};
use crate::ContentPolicy;
use crate::apply::AppliedMove;

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
            .arg(super::git_directory_argument(&repository))
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
        |_| Ok(()),
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
    let committed_document = git.canonical.join(format!(
        "projects/fictional-project/experiments/2026-07-31-{DOCUMENT_ID}/index.md"
    ));
    if let Err(error) = fs::write(&committed_document, b"partial fictional checkout\n") {
        panic!("partial canonical checkout fixture must be written: {error}");
    }
    assert!(matches!(
        repository.recover_batch(&worker, parse_batch_id()),
        Ok(BatchCommitOutcome::Committed {
            commit: recovered,
            ..
        }) if recovered == commit
    ));
    assert_eq!(
        fs::read_to_string(&committed_document)
            .unwrap_or_else(|error| panic!("repaired document must be readable: {error}")),
        markdown(FIRST_REQUEST_ID, "Fictional experiment")
    );
    if let Err(error) = repository.finalize_batch(&worker, parse_batch_id(), Some(&commit)) {
        panic!("durably reconciled batch must finalize: {error}");
    }
    assert_eq!(git.transaction_count(), 0);
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
        |_| Ok(()),
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
    if let Err(error) = repository.finalize_batch(&worker, parse_batch_id(), None) {
        panic!("durably failed batch must finalize: {error}");
    }
    assert_eq!(git.transaction_count(), 0);
}

#[test]
fn no_changes_recovery_rejects_a_stale_official_base() {
    let root = TestDirectory::new();
    let git = GitFixture::initialize(root.path());
    let base = git.official_commit();
    let (queue, mut worker) = queue_and_worker(&root.path().join("queue"));
    let (request, payload) = missing_update_request();
    let claim = enqueue_and_claim(&queue, &mut worker, FIRST_REQUEST_ID, &request, &payload);
    assert!(matches!(
        git.open().apply_batch(
            &mut worker,
            parse_batch_id(),
            &[claim],
            ContentPolicy::default(),
            &PackagePolicy::default(),
            |_| Ok(()),
        ),
        Ok(BatchCommitOutcome::NoChanges { .. })
    ));
    let concurrent = commit_empty_canonical(&git.canonical, "Advance fictional official state");
    assert!(matches!(
        git.open().recover_batch(&worker, parse_batch_id()),
        Err(GitTransactionError::OfficialBranchChanged { expected, actual })
            if expected == base && actual == concurrent
    ));
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
            |_| Ok(()),
        ),
        Ok(BatchCommitOutcome::NoChanges { failures })
            if failures.len() == 1
                && failures[0].error_code() == ErrorCode::InvalidRequest
    ));
}

#[test]
fn trial_build_failure_keeps_the_official_commit_unchanged() {
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
            "Create a build-breaking fictional experiment",
        ),
        &markdown(FIRST_REQUEST_ID, "Build-breaking fictional experiment"),
    );
    assert!(matches!(
        git.open().apply_batch(
            &mut worker,
            parse_batch_id(),
            &[claim],
            ContentPolicy::default(),
            &PackagePolicy::default(),
            |_| Err(GitTransactionError::TrialBuildFailed),
        ),
        Err(GitTransactionError::TrialBuildFailed)
    ));
    assert_eq!(git.official_commit(), base);
    assert_eq!(git.transaction_count(), 1);
    assert_eq!(git.worktree_count(), 1);
}

#[test]
fn trial_build_cannot_change_the_tree_that_will_be_committed() {
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
    let result = git.open().apply_batch(
        &mut worker,
        parse_batch_id(),
        &[claim],
        ContentPolicy::default(),
        &PackagePolicy::default(),
        |worktree| {
            fs::write(worktree.join("generated.html"), "<p>unexpected</p>\n")
                .map_err(GitTransactionError::Io)
        },
    );
    assert!(matches!(
        result,
        Err(GitTransactionError::TrialBuildMutatedWorktree)
    ));
    assert_eq!(git.official_commit(), base);
}

#[test]
fn rejects_executable_repository_local_git_configuration() {
    let root = TestDirectory::new();
    let fixture = GitFixture::initialize(root.path());
    git(
        None,
        [
            "--git-dir",
            fixture
                .repository
                .to_str()
                .unwrap_or("invalid-fictional-repository"),
            "config",
            "core.fsmonitor",
            "/fictional/fsmonitor",
        ],
        None,
    );
    let identity = GitIdentity::new("Agent Knowledge Worker", "agent-knowledge@example.invalid")
        .unwrap_or_else(|error| panic!("identity must be valid: {error}"));
    assert!(matches!(
        GitRepository::open(
            &fixture.repository,
            &fixture.canonical,
            &fixture.work,
            "main",
            identity,
        ),
        Err(GitTransactionError::UnsafeGitConfig)
    ));
}

#[test]
fn rejects_worktree_config_and_a_second_work_root() {
    let root = TestDirectory::new();
    let fixture = GitFixture::initialize(root.path());
    let repository = fixture.open();
    drop(repository);
    git(
        None,
        [
            "--git-dir",
            fixture
                .repository
                .to_str()
                .unwrap_or("invalid-fictional-repository"),
            "config",
            "remote.backup.eu.url",
            "ssh://fictional.invalid/knowledge.git",
        ],
        None,
    );
    drop(fixture.open());
    let identity = GitIdentity::new("Agent Knowledge Worker", "agent-knowledge@example.invalid")
        .unwrap_or_else(|error| panic!("identity must be valid: {error}"));
    let other_work = root.path().join("other-work");
    fs::create_dir(&other_work)
        .unwrap_or_else(|error| panic!("other work root must be created: {error}"));
    assert!(matches!(
        GitRepository::open(
            &fixture.repository,
            &fixture.canonical,
            &other_work,
            "main",
            identity.clone(),
        ),
        Err(GitTransactionError::RepositoryBindingMismatch)
    ));
    git(
        None,
        [
            "--git-dir",
            fixture
                .repository
                .to_str()
                .unwrap_or("invalid-fictional-repository"),
            "branch",
            "alternate",
            "main",
        ],
        None,
    );
    let alternate = root.path().join("alternate-content");
    let status = Command::new("git")
        .arg(super::git_directory_argument(&fixture.repository))
        .args(["worktree", "add"])
        .arg(&alternate)
        .arg("alternate")
        .status();
    assert!(
        matches!(status, Ok(status) if status.success()),
        "alternate canonical worktree must be created"
    );
    assert!(matches!(
        GitRepository::open(
            &fixture.repository,
            &alternate,
            &fixture.work,
            "alternate",
            identity.clone(),
        ),
        Err(GitTransactionError::RepositoryBindingMismatch)
    ));
    git(
        None,
        [
            "--git-dir",
            fixture
                .repository
                .to_str()
                .unwrap_or("invalid-fictional-repository"),
            "config",
            "extensions.worktreeConfig",
            "true",
        ],
        None,
    );
    assert!(matches!(
        GitRepository::open(
            &fixture.repository,
            &fixture.canonical,
            &fixture.work,
            "main",
            identity,
        ),
        Err(GitTransactionError::UnsafeGitConfig)
    ));
}

#[test]
fn rejects_a_canonical_worktree_subdirectory() {
    let root = TestDirectory::new();
    let fixture = GitFixture::initialize(root.path());
    let nested = fixture.canonical.join("nested");
    fs::create_dir(&nested)
        .unwrap_or_else(|error| panic!("nested worktree directory must be created: {error}"));
    let identity = GitIdentity::new("Agent Knowledge Worker", "agent-knowledge@example.invalid")
        .unwrap_or_else(|error| panic!("identity must be valid: {error}"));

    assert!(matches!(
        GitRepository::open(
            &fixture.repository,
            &nested,
            &fixture.work,
            "main",
            identity,
        ),
        Err(GitTransactionError::CanonicalWorktreeMismatch)
    ));
}

#[test]
fn revalidates_core_repository_configuration_under_the_writer_lock() {
    let root = TestDirectory::new();
    let fixture = GitFixture::initialize(root.path());
    let repository = fixture.open();
    git(
        None,
        [
            "--git-dir",
            fixture
                .repository
                .to_str()
                .unwrap_or("invalid-fictional-repository"),
            "config",
            "core.filemode",
            "false",
        ],
        None,
    );

    assert!(matches!(
        repository.lock_writer(),
        Err(GitTransactionError::UnsafeGitConfig)
    ));
}

#[cfg(unix)]
#[test]
fn preserves_non_utf8_git_directory_arguments() {
    let path = PathBuf::from(OsString::from_vec(
        b"/tmp/fictional-git-directory-\xff".to_vec(),
    ));
    let argument = super::git_directory_argument(&path);
    let mut expected = b"--git-dir=".to_vec();
    expected.extend_from_slice(path.as_os_str().as_bytes());
    assert_eq!(argument.as_os_str().as_bytes(), expected);
}

#[cfg(unix)]
#[test]
fn opens_a_repository_below_a_non_utf8_path() {
    let root = TestDirectory::new();
    let parent = root
        .path()
        .join(OsString::from_vec(b"fictional-repository-\xff".to_vec()));
    fs::create_dir(&parent)
        .unwrap_or_else(|error| panic!("non-UTF-8 fixture parent must be created: {error}"));
    let fixture = GitFixture::initialize(&parent);
    drop(fixture.open());
}

#[cfg(unix)]
#[test]
fn stores_only_canonical_repository_paths() {
    let root = TestDirectory::new();
    let actual = root.path().join("actual");
    fs::create_dir(&actual)
        .unwrap_or_else(|error| panic!("actual fixture directory must be created: {error}"));
    let fixture = GitFixture::initialize(&actual);
    let alias = root.path().join("alias");
    std::os::unix::fs::symlink(&actual, &alias)
        .unwrap_or_else(|error| panic!("fixture alias must be created: {error}"));
    let identity = GitIdentity::new("Agent Knowledge Worker", "agent-knowledge@example.invalid")
        .unwrap_or_else(|error| panic!("identity must be valid: {error}"));
    let repository = GitRepository::open(
        &alias.join("repository"),
        &alias.join("content"),
        &alias.join("work"),
        "main",
        identity,
    )
    .unwrap_or_else(|error| panic!("repository must open through a path alias: {error}"));

    assert_eq!(
        repository.git_directory,
        fs::canonicalize(&fixture.repository)
            .unwrap_or_else(|error| panic!("repository path must canonicalize: {error}"))
    );
    assert_eq!(
        repository.canonical_worktree,
        fs::canonicalize(&fixture.canonical)
            .unwrap_or_else(|error| panic!("worktree path must canonicalize: {error}"))
    );
    assert_eq!(
        repository.work_root,
        fs::canonicalize(&fixture.work)
            .unwrap_or_else(|error| panic!("work root must canonicalize: {error}"))
    );
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
        TransactionHooks {
            trial_build: accept_trial_build,
            before_publish: interrupt_publication,
        },
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
    if let Err(error) = fs::write(&journal_path, &original_journal) {
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
    if let Err(error) = repository.finalize_batch(&worker, parse_batch_id(), Some(&commit)) {
        panic!("resumed batch must finalize: {error}");
    }
    assert_eq!(git.transaction_count(), 0);
}

#[test]
fn counts_an_explicitly_moved_and_rewritten_file_without_git_rename_detection() {
    let root = TestDirectory::new();
    let fixture = GitFixture::initialize(root.path());
    let source_bundle = PathBuf::from("projects/fictional-a/runbooks/source");
    let destination_bundle = PathBuf::from("projects/fictional-a/archive/runbooks/source");
    let source = source_bundle.join("index.md");
    let destination = destination_bundle.join("index.md");
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
        &[AppliedMove {
            source: source_bundle,
            destination: destination_bundle,
        }],
    ) {
        Ok(stats) => stats,
        Err(error) => panic!("explicit move must authorize the paired deletion: {error}"),
    };
    assert_eq!(stats.added, 0);
    assert_eq!(stats.modified, 1);
    assert_eq!(stats.deleted, 0);
}

#[test]
fn replays_ordered_moves_when_a_source_path_is_reused() {
    let root = TestDirectory::new();
    let fixture = GitFixture::initialize(root.path());
    let first_bundle = PathBuf::from("projects/fictional-a/runbooks/source");
    let second_bundle = PathBuf::from("projects/fictional-b/runbooks/source");
    let final_bundle = PathBuf::from("projects/fictional-c/runbooks/source");
    let first = first_bundle.join("index.md");
    let second = second_bundle.join("index.md");
    let final_path = final_bundle.join("index.md");
    write_tracked_fixture(
        &fixture.canonical,
        &first,
        b"Fictional repeatedly moved document.\n",
    );
    let base = commit_canonical_fixture(&fixture.canonical, "Add repeatedly moved document");

    move_fixture(&fixture.canonical, &first, &second);
    move_fixture(&fixture.canonical, &second, &first);
    move_fixture(&fixture.canonical, &first, &final_path);
    git(Some(&fixture.canonical), ["add", "--all"], None);

    let stats = match staged_stats(
        &fixture.canonical,
        &base,
        &[
            AppliedMove {
                source: first_bundle.clone(),
                destination: second_bundle.clone(),
            },
            AppliedMove {
                source: second_bundle,
                destination: first_bundle.clone(),
            },
            AppliedMove {
                source: first_bundle,
                destination: final_bundle,
            },
        ],
    ) {
        Ok(stats) => stats,
        Err(error) => panic!("ordered path reuse must remain a valid move: {error}"),
    };
    assert_eq!(stats.added, 0);
    assert_eq!(stats.modified, 1);
    assert_eq!(stats.deleted, 0);
}

fn move_fixture(worktree: &Path, source: &Path, destination: &Path) {
    let destination = worktree.join(destination);
    if let Some(parent) = destination.parent()
        && let Err(error) = fs::create_dir_all(parent)
    {
        panic!("move fixture parent must be created: {error}");
    }
    if let Err(error) = fs::rename(worktree.join(source), destination) {
        panic!("move fixture must be renamed: {error}");
    }
}

#[test]
fn interrupted_reset_allows_only_exact_base_tree_residue() {
    let root = TestDirectory::new();
    let fixture = GitFixture::initialize(root.path());
    let source = PathBuf::from("projects/fictional-a/runbooks/source/index.md");
    let destination = PathBuf::from("projects/fictional-a/archive/runbooks/source/index.md");
    let bytes = b"Fictional pre-reset bytes.\n";
    write_tracked_fixture(&fixture.canonical, &source, bytes);
    let base = commit_canonical_fixture(&fixture.canonical, "Add reset source");
    move_fixture(&fixture.canonical, &source, &destination);
    let _target = commit_canonical_fixture(&fixture.canonical, "Move reset source");
    write_tracked_fixture(&fixture.canonical, &source, bytes);
    let repository = fixture.open();
    if let Err(error) = repository.ensure_canonical_repairable(&base) {
        panic!("exact base residue must be repairable: {error}");
    }
    if let Err(error) = fs::write(fixture.canonical.join(&source), b"unrelated bytes\n") {
        panic!("changed residue fixture must be written: {error}");
    }
    assert!(matches!(
        repository.ensure_canonical_repairable(&base),
        Err(GitTransactionError::CanonicalWorktreeDirty)
    ));
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

fn commit_empty_canonical(worktree: &Path, message: &str) -> String {
    let status = Command::new("git")
        .current_dir(worktree)
        .args([
            "-c",
            "user.name=Agent Knowledge Test",
            "-c",
            "user.email=agent-knowledge@example.invalid",
            "commit",
            "--allow-empty",
            "-m",
            message,
        ])
        .status();
    match status {
        Ok(status) if status.success() => {}
        Ok(status) => panic!("empty canonical fixture commit failed with {status}"),
        Err(error) => panic!("empty canonical fixture commit command failed: {error}"),
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
            |_| Ok(()),
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
            |_| Ok(()),
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
        |_| Ok(()),
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
            |_| Ok(()),
        ),
        Err(GitTransactionError::UnfinishedTransaction)
    ));
    if let Err(error) = repository.finalize_batch(&worker, parse_batch_id(), Some(&first_commit)) {
        panic!("first batch must finalize before the next publication: {error}");
    }
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
            |_| Ok(()),
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
            |_| Ok(()),
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
        TransactionHooks {
            trial_build: accept_trial_build,
            before_publish: move |expected: &str, _: &str| {
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

#[test]
fn parses_supported_git_version_formats() {
    assert!(matches!(
        parse_git_version(b"git version 2.36.0\n"),
        Ok((2, 36))
    ));
    assert!(matches!(
        parse_git_version(b"git version 2.55.0.windows.1\n"),
        Ok((2, 55))
    ));
    assert!(matches!(
        parse_git_version(b"fictional version"),
        Err(GitTransactionError::InvalidGitOutput)
    ));
}

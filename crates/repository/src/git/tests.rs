use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::process::Command;

use agent_knowledge_core::{BatchId, ErrorCode, PayloadPath, RequestId, Revision};
use agent_knowledge_queue::{
    ClaimedPackage, FileQueue, PackagePolicy, ProcessingScanOutcome, WorkerSession,
};
use ulid::Ulid;

use super::{BatchCommitOutcome, GitIdentity, GitRepository, GitTransactionError, parse_text};
use crate::ContentPolicy;

const FIRST_REQUEST_ID: &str = "01K00000000000000000000001";
const SECOND_REQUEST_ID: &str = "01K00000000000000000000002";
const DOCUMENT_ID: &str = "01K00000000000000000000003";
const BATCH_ID: &str = "01K00000000000000000000004";

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
    match BATCH_ID.parse() {
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
    match worker.claim(parse_request_id(request_id), parse_batch_id()) {
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

    let outcome =
        match git
            .open()
            .apply_batch(parse_batch_id(), &[first, second], ContentPolicy::default())
        {
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
            commit,
        ],
    );
    assert!(message.contains("knowledge snapshot: 1 changes"));
    assert!(message.contains(&format!("Request-ID: {FIRST_REQUEST_ID}")));
    assert!(!message.contains(&format!("Request-ID: {SECOND_REQUEST_ID}")));
    assert_eq!(
        fs::read_dir(&git.work)
            .map(|entries| entries.count())
            .unwrap_or(usize::MAX),
        0
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
    let outcome = match git
        .open()
        .apply_batch(parse_batch_id(), &[claim], ContentPolicy::default())
    {
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
    assert_eq!(
        fs::read_dir(&git.work)
            .map(|entries| entries.count())
            .unwrap_or(usize::MAX),
        0
    );
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
        parse_batch_id(),
        &[claim],
        ContentPolicy::default(),
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

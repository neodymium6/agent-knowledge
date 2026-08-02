use std::fs;
use std::io::{self, Cursor, Read};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

use agent_knowledge_core::ErrorCode;
use agent_knowledge_protocol::{
    ClientId, ExportRequest, GetRequest, ListRequest, ReadFilterRequest, RequestStatus,
    SearchRequest, StatusRequest, SubmitOutcome,
};
use agent_knowledge_queue::{PackagePolicy, validate_accepted_package};
use tar::{Builder, EntryType, Header};

use super::{
    ArchiveError, GatewayError, GatewaySettings, ReadGateway, StatusGateway, SubmitGateway,
};

const REQUEST_ID: &str = "01K00000000000000000000000";
const REQUEST_JSON: &[u8] = br#"{
  "protocol_version": 1,
  "request_id": "01K00000000000000000000000",
  "title": "Record a fictional Gateway test",
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
title: Fictional Gateway test\n\
created: 2026-07-31T03:50:00Z\n\
request_id: 01K00000000000000000000000\n\
status: active\n\
---\n\
Fictional Gateway body.\n";

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn create() -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "agent-knowledge-gateway-test-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path)
            .unwrap_or_else(|error| panic!("Gateway test directory must be created: {error}"));
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
            panic!("Gateway test directory must be removed: {error}");
        }
    }
}

fn gateway(root: &TestDirectory) -> SubmitGateway {
    let settings = settings(root);
    SubmitGateway::open(&settings)
        .unwrap_or_else(|error| panic!("submit Gateway must open: {error}"))
}

fn read_gateway(root: &TestDirectory) -> ReadGateway {
    initialize_committed_content(root);
    let settings = settings(root);
    ReadGateway::open_until(&settings, None)
        .unwrap_or_else(|error| panic!("read Gateway must open: {error}"))
}

fn settings(root: &TestDirectory) -> GatewaySettings {
    let yaml = format!(
        "schema_version: 2\nstorage:\n  queue_root: {}\n  git_directory: {}\n  content_root: {}\nrepository:\n  official_branch: main\nreads:\n  maximum_results: 100\n  maximum_query_characters: 512\n  maximum_index_entries: 100000\n  maximum_index_markdown_bytes: 536870912\n  maximum_search_documents: 10000\n  maximum_search_markdown_bytes: 536870912\n  operation_timeout_seconds: 30\n  maximum_response_bytes: 268435456\n  search_metadata:\n    node: true\n    agent: true\n    session: true\n    request_id: true\ntransport:\n  submit_timeout_seconds: 300\n",
        root.path().join("queue").display(),
        root.path().join("repository").display(),
        root.path().join("content").display(),
    );
    GatewaySettings::decode(&yaml)
        .unwrap_or_else(|error| panic!("Gateway settings must decode: {error}"))
}

fn client_id() -> ClientId {
    "fictional-node-a"
        .parse()
        .unwrap_or_else(|error| panic!("client ID fixture must parse: {error}"))
}

fn initialize_committed_content(root: &TestDirectory) {
    let repository = root.path().join("repository");
    let seed = root.path().join("seed");
    let content = root.path().join("content");
    run_git(None, &["init", "--bare", path_text(&repository)]);
    run_git(None, &["init", "--initial-branch=main", path_text(&seed)]);
    run_git(Some(&seed), &["config", "user.name", "Fictional Writer"]);
    run_git(
        Some(&seed),
        &["config", "user.email", "writer@fictional.invalid"],
    );
    let document = seed
        .join("projects/fictional-project/runbooks/2026-07-31-01K00000000000000000000001/index.md");
    fs::create_dir_all(
        document
            .parent()
            .unwrap_or_else(|| panic!("document fixture must have a parent")),
    )
    .unwrap_or_else(|error| panic!("document fixture directory must be created: {error}"));
    fs::write(
        &document,
        "---\nschema_version: 1\ndocument_id: 01K00000000000000000000001\ntitle: Fictional restart guide\ncreated: 2026-07-31T03:50:00Z\nrequest_id: 01K00000000000000000000002\ntags:\n  - operations\nstatus: active\n---\nRestart the fictional service safely.\n",
    )
    .unwrap_or_else(|error| panic!("document fixture must be written: {error}"));
    fs::write(
        document
            .parent()
            .unwrap_or_else(|| panic!("document fixture must have a parent"))
            .join("procedure.json"),
        b"{\"fictional\":true}\n",
    )
    .unwrap_or_else(|error| panic!("attachment fixture must be written: {error}"));
    run_git(Some(&seed), &["add", "."]);
    run_git(
        Some(&seed),
        &["commit", "-m", "Initialize fictional knowledge"],
    );
    run_git(
        Some(&seed),
        &["remote", "add", "origin", path_text(&repository)],
    );
    run_git(Some(&seed), &["push", "origin", "main"]);
    run_git(
        None,
        &[
            "--git-dir",
            path_text(&repository),
            "symbolic-ref",
            "HEAD",
            "refs/heads/main",
        ],
    );
    run_git(
        None,
        &[
            "--git-dir",
            path_text(&repository),
            "worktree",
            "add",
            path_text(&content),
            "main",
        ],
    );
}

fn path_text(path: &Path) -> &str {
    path.to_str()
        .unwrap_or_else(|| panic!("fixture path must be UTF-8"))
}

fn run_git(working_directory: Option<&Path>, arguments: &[&str]) {
    let mut command = Command::new("git");
    if let Some(directory) = working_directory {
        command.current_dir(directory);
    }
    let output = command
        .args(arguments)
        .output()
        .unwrap_or_else(|error| panic!("Git fixture must run: {error}"));
    if !output.status.success() {
        panic!(
            "Git fixture failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn serves_list_recent_get_and_search_from_one_committed_revision() {
    let root = TestDirectory::create();
    let gateway = read_gateway(&root);
    let filter = ReadFilterRequest {
        project: Some(
            "fictional-project"
                .parse()
                .unwrap_or_else(|error| panic!("project fixture must parse: {error}")),
        ),
        ..ReadFilterRequest::default()
    };
    let list = gateway
        .list(&ListRequest::new(filter.clone(), 10))
        .unwrap_or_else(|error| panic!("committed list must succeed: {error}"));
    assert_eq!(list.documents.len(), 1);
    assert!(!list.commit.is_empty());
    assert_eq!(
        gateway
            .recent(&ListRequest::new(filter.clone(), 10))
            .unwrap_or_else(|error| panic!("committed recent must succeed: {error}"))
            .commit,
        list.commit
    );
    let document_id = "01K00000000000000000000001"
        .parse()
        .unwrap_or_else(|error| panic!("document fixture must parse: {error}"));
    let get = gateway
        .get(GetRequest::new(document_id))
        .unwrap_or_else(|error| panic!("committed get must succeed: {error}"));
    assert!(get.document.markdown.contains("fictional service safely"));
    let search = gateway
        .search(&SearchRequest::new("restart".into(), filter, 10))
        .unwrap_or_else(|error| panic!("committed search must succeed: {error}"));
    assert_eq!(search.documents.len(), 1);
    assert_eq!(search.commit, list.commit);
}

#[test]
fn exports_a_deterministic_committed_document_bundle() {
    let root = TestDirectory::create();
    let gateway = read_gateway(&root);
    let document_id = "01K00000000000000000000001"
        .parse()
        .unwrap_or_else(|error| panic!("document fixture must parse: {error}"));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let archive = gateway
        .export_encoded_until(ExportRequest::new(document_id), deadline)
        .unwrap_or_else(|error| panic!("committed export must succeed: {error}"));
    let mut entries = tar::Archive::new(Cursor::new(archive))
        .entries()
        .unwrap_or_else(|error| panic!("export archive must decode: {error}"))
        .map(|entry| {
            let mut entry =
                entry.unwrap_or_else(|error| panic!("export entry must decode: {error}"));
            let path = entry
                .path()
                .unwrap_or_else(|error| panic!("export path must decode: {error}"))
                .into_owned();
            let mut bytes = Vec::new();
            entry
                .read_to_end(&mut bytes)
                .unwrap_or_else(|error| panic!("export entry must read: {error}"));
            (path, bytes)
        })
        .collect::<Vec<_>>();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries.remove(0).0, Path::new("index.md"));
    assert_eq!(entries[0].0, Path::new("procedure.json"));
    assert_eq!(entries[0].1, b"{\"fictional\":true}\n");
}

#[cfg(target_family = "unix")]
#[test]
fn committed_reads_do_not_open_the_queue_path() {
    use std::os::unix::net::UnixListener;

    let root = TestDirectory::create();
    initialize_committed_content(&root);
    let queue_socket = UnixListener::bind(root.path().join("queue"))
        .unwrap_or_else(|error| panic!("fictional queue socket must bind: {error}"));
    let settings = settings(&root);
    let gateway = ReadGateway::open_until(&settings, None)
        .unwrap_or_else(|error| panic!("read Gateway must ignore the queue path: {error}"));
    let response = gateway
        .list(&ListRequest::new(ReadFilterRequest::default(), 10))
        .unwrap_or_else(|error| panic!("committed list must succeed: {error}"));
    assert_eq!(response.documents.len(), 1);
    drop(queue_socket);
}

#[cfg(target_family = "unix")]
#[test]
fn request_status_reads_only_the_existing_queue() {
    use std::os::unix::net::UnixListener;

    let root = TestDirectory::create();
    let settings = settings(&root);
    SubmitGateway::open(&settings)
        .unwrap_or_else(|error| panic!("submit Gateway fixture must open: {error}"))
        .submit(client_id(), Cursor::new(valid_archive()))
        .unwrap_or_else(|error| panic!("Gateway archive fixture must be accepted: {error}"));
    let repository_socket = UnixListener::bind(settings.git_directory())
        .unwrap_or_else(|error| panic!("fictional repository socket must bind: {error}"));
    let content_socket = UnixListener::bind(settings.content_root())
        .unwrap_or_else(|error| panic!("fictional content socket must bind: {error}"));

    let gateway = StatusGateway::open_until(&settings, None)
        .unwrap_or_else(|error| panic!("status Gateway must ignore committed storage: {error}"));
    let request_id = REQUEST_ID
        .parse()
        .unwrap_or_else(|error| panic!("request ID fixture must parse: {error}"));
    let response = gateway
        .status(StatusRequest::new(request_id))
        .unwrap_or_else(|error| panic!("pending request status must succeed: {error}"));
    assert_eq!(response.request_id(), request_id);
    assert_eq!(response.request_status(), RequestStatus::Pending);

    let missing = "01K00000000000000000000099"
        .parse()
        .unwrap_or_else(|error| panic!("missing request ID fixture must parse: {error}"));
    assert!(matches!(
        gateway.status(StatusRequest::new(missing)),
        Err(GatewayError::RequestNotFound { request_id }) if request_id == missing
    ));
    drop((repository_socket, content_socket));
}

#[test]
fn rejects_overlapping_storage_before_initializing_the_queue() {
    let root = TestDirectory::create();
    initialize_committed_content(&root);
    let content = root.path().join("content");
    let yaml = format!(
        "schema_version: 2\nstorage:\n  queue_root: {}\n  git_directory: {}\n  content_root: {}\nrepository:\n  official_branch: main\nreads:\n  maximum_results: 100\n  maximum_query_characters: 512\n  maximum_index_entries: 100000\n  maximum_index_markdown_bytes: 536870912\n  maximum_search_documents: 10000\n  maximum_search_markdown_bytes: 536870912\n  operation_timeout_seconds: 30\n  maximum_response_bytes: 268435456\n  search_metadata:\n    node: true\n    agent: true\n    session: true\n    request_id: true\ntransport:\n  submit_timeout_seconds: 300\n",
        content.display(),
        root.path().join("repository").display(),
        content.display(),
    );
    let settings = GatewaySettings::decode(&yaml)
        .unwrap_or_else(|error| panic!("overlap settings must decode: {error}"));
    assert!(matches!(
        SubmitGateway::open(&settings),
        Err(GatewayError::OverlappingStorage)
    ));
    assert!(!content.join("queue-id").exists());
}

fn append_entry(builder: &mut Builder<Vec<u8>>, path: &str, kind: EntryType, contents: &[u8]) {
    let mut header = Header::new_gnu();
    header.set_entry_type(kind);
    header.set_mode(if kind == EntryType::Directory {
        0o755
    } else {
        0o644
    });
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header.set_size(contents.len() as u64);
    if kind == EntryType::GNUSparse {
        let Some(gnu) = header.as_gnu_mut() else {
            panic!("GNU sparse fixture must use a GNU header");
        };
        gnu.set_real_size(contents.len() as u64);
    }
    header.set_cksum();
    builder
        .append_data(&mut header, path, Cursor::new(contents))
        .unwrap_or_else(|error| panic!("tar fixture entry must append: {error}"));
}

fn valid_archive() -> Vec<u8> {
    let mut builder = Builder::new(Vec::new());
    append_entry(&mut builder, "payload", EntryType::Directory, &[]);
    append_entry(&mut builder, "payload/run", EntryType::Directory, &[]);
    append_entry(
        &mut builder,
        "payload/run/index.md",
        EntryType::Regular,
        MARKDOWN,
    );
    append_entry(
        &mut builder,
        "request.json",
        EntryType::Regular,
        REQUEST_JSON,
    );
    builder
        .into_inner()
        .unwrap_or_else(|error| panic!("tar fixture must finish: {error}"))
}

#[test]
fn streams_a_valid_archive_and_preserves_authenticated_identity() {
    let root = TestDirectory::create();
    assert!(!root.path().join("repository").exists());
    let gateway = gateway(&root);
    assert!(!root.path().join("repository").exists());
    let response = gateway
        .submit(client_id(), Cursor::new(valid_archive()))
        .unwrap_or_else(|error| panic!("valid archive must be accepted: {error}"));
    assert!(matches!(response.outcome, SubmitOutcome::Accepted { .. }));

    let accepted = root.path().join(format!("queue/pending/{REQUEST_ID}"));
    let package = validate_accepted_package(&accepted, &PackagePolicy::default())
        .unwrap_or_else(|error| panic!("accepted package must validate: {error}"));
    assert_eq!(
        package
            .acceptance()
            .and_then(|metadata| metadata.client_id.as_ref())
            .map(ClientId::as_str),
        Some("fictional-node-a")
    );

    let retry = gateway
        .submit(client_id(), Cursor::new(valid_archive()))
        .unwrap_or_else(|error| panic!("identical retry must succeed: {error}"));
    assert!(matches!(retry.outcome, SubmitOutcome::Existing { .. }));
}

#[test]
fn rejects_duplicate_special_and_extended_entries() {
    let root = TestDirectory::create();
    let gateway = gateway(&root);

    let mut duplicate = Builder::new(Vec::new());
    append_entry(
        &mut duplicate,
        "request.json",
        EntryType::Regular,
        REQUEST_JSON,
    );
    append_entry(
        &mut duplicate,
        "request.json",
        EntryType::Regular,
        REQUEST_JSON,
    );
    let duplicate = duplicate
        .into_inner()
        .unwrap_or_else(|error| panic!("duplicate tar fixture must finish: {error}"));
    assert!(matches!(
        gateway.submit(client_id(), Cursor::new(duplicate)),
        Err(GatewayError::Archive(ArchiveError::DuplicatePath))
    ));

    let mut special = Builder::new(Vec::new());
    append_entry(
        &mut special,
        "request.json",
        EntryType::Regular,
        REQUEST_JSON,
    );
    append_entry(&mut special, "payload/link", EntryType::Symlink, &[]);
    let special = special
        .into_inner()
        .unwrap_or_else(|error| panic!("special tar fixture must finish: {error}"));
    assert!(matches!(
        gateway.submit(client_id(), Cursor::new(special)),
        Err(GatewayError::Archive(ArchiveError::InvalidEntryType))
    ));

    let mut extended = Builder::new(Vec::new());
    extended
        .append_pax_extensions([("comment", b"fictional".as_slice())])
        .unwrap_or_else(|error| panic!("PAX fixture must append: {error}"));
    append_entry(
        &mut extended,
        "request.json",
        EntryType::Regular,
        REQUEST_JSON,
    );
    let extended = extended
        .into_inner()
        .unwrap_or_else(|error| panic!("PAX tar fixture must finish: {error}"));
    assert!(matches!(
        gateway.submit(client_id(), Cursor::new(extended)),
        Err(GatewayError::Archive(ArchiveError::UnsupportedExtension))
    ));
}

#[test]
fn rejects_sparse_empty_directory_and_trailing_data() {
    let root = TestDirectory::create();
    let gateway = gateway(&root);

    let mut sparse = Builder::new(Vec::new());
    append_entry(
        &mut sparse,
        "request.json",
        EntryType::Regular,
        REQUEST_JSON,
    );
    append_entry(&mut sparse, "payload/sparse.bin", EntryType::GNUSparse, &[]);
    let sparse = sparse
        .into_inner()
        .unwrap_or_else(|error| panic!("sparse tar fixture must finish: {error}"));
    let sparse_error = match gateway.submit(client_id(), Cursor::new(sparse)) {
        Ok(_) => panic!("sparse entry must fail"),
        Err(error) => error,
    };
    assert!(
        matches!(
            sparse_error,
            GatewayError::Archive(ArchiveError::SparseEntry)
        ),
        "unexpected sparse-entry failure: {sparse_error:?}"
    );

    let mut empty = Builder::new(Vec::new());
    append_entry(&mut empty, "request.json", EntryType::Regular, REQUEST_JSON);
    append_entry(&mut empty, "payload/unused", EntryType::Directory, &[]);
    let empty = empty
        .into_inner()
        .unwrap_or_else(|error| panic!("empty-directory tar fixture must finish: {error}"));
    assert!(matches!(
        gateway.submit(client_id(), Cursor::new(empty)),
        Err(GatewayError::Archive(ArchiveError::EmptyDirectory))
    ));

    let mut trailing = valid_archive();
    trailing.extend_from_slice(b"not-zero-padding");
    let error = match gateway.submit(client_id(), Cursor::new(trailing)) {
        Ok(_) => panic!("nonzero trailing data must fail"),
        Err(error) => error,
    };
    assert_eq!(error.error_code(), ErrorCode::InvalidRequest);
    assert!(matches!(
        error,
        GatewayError::Archive(ArchiveError::TrailingData)
    ));
}

#[test]
fn distinguishes_transport_failures_from_malformed_archives() {
    struct FailingInput;

    impl Read for FailingInput {
        fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "fictional connection reset",
            ))
        }
    }

    let root = TestDirectory::create();
    let gateway = gateway(&root);
    let transport = match gateway.submit(client_id(), FailingInput) {
        Ok(_) => panic!("transport failure must not accept a package"),
        Err(error) => error,
    };
    assert_eq!(transport.error_code(), ErrorCode::TemporaryFailure);

    let malformed = match gateway.submit(client_id(), Cursor::new(b"not a tar archive")) {
        Ok(_) => panic!("malformed tar must not accept a package"),
        Err(error) => error,
    };
    assert_eq!(malformed.error_code(), ErrorCode::InvalidRequest);
}

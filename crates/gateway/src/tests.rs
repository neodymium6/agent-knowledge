use std::fs;
use std::io::{self, Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use agent_knowledge_core::ErrorCode;
use agent_knowledge_protocol::{ClientId, SubmitOutcome};
use agent_knowledge_queue::{PackagePolicy, validate_accepted_package};
use tar::{Builder, EntryType, Header};

use super::{ArchiveError, Gateway, GatewayError, GatewaySettings};

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

fn gateway(root: &TestDirectory) -> Gateway {
    let yaml = format!(
        "schema_version: 1\nstorage:\n  queue_root: {}\n",
        root.path().join("queue").display()
    );
    let settings = GatewaySettings::decode(&yaml)
        .unwrap_or_else(|error| panic!("Gateway settings must decode: {error}"));
    Gateway::open(&settings).unwrap_or_else(|error| panic!("Gateway must open: {error}"))
}

fn client_id() -> ClientId {
    "fictional-node-a"
        .parse()
        .unwrap_or_else(|error| panic!("client ID fixture must parse: {error}"))
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
    let gateway = gateway(&root);
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

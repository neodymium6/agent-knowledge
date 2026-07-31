use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use agent_knowledge_core::Revision;
use agent_knowledge_queue::{PackagePolicy, ValidatedPackage, validate_package};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use ulid::Ulid;

use super::{ApplyError, apply_request};
use crate::ContentPolicy;

const DOCUMENT_ID: &str = "01K00000000000000000000000";
const CREATE_REQUEST_ID: &str = "01K00000000000000000000001";
const UPDATE_REQUEST_ID: &str = "01K00000000000000000000002";
const MOVE_REQUEST_ID: &str = "01K00000000000000000000003";
const ARCHIVE_REQUEST_ID: &str = "01K00000000000000000000004";
const ORIGINAL_REQUEST_ID: &str = "01K00000000000000000000005";
const SESSION_ID: &str = "01K00000000000000000000006";

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "agent-knowledge-repository-apply-test-{}",
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

fn document(document_id: &str, request_id: &str, title: &str, updated: Option<&str>) -> String {
    let updated = updated.map_or_else(String::new, |value| format!("updated: {value}\n"));
    format!(
        "---\n\
         schema_version: 1\n\
         document_id: {document_id}\n\
         title: {title}\n\
         created: 2026-07-31T03:50:00Z\n\
         {updated}\
         request_id: {request_id}\n\
         status: active\n\
         ---\n\
         Fictional body for {title}.\n"
    )
}

fn log_document(request_id: &str, updated: Option<&str>) -> String {
    let updated = updated.map_or_else(String::new, |value| format!("updated: {value}\n"));
    format!(
        "---\n\
         schema_version: 1\n\
         document_id: {DOCUMENT_ID}\n\
         title: Fictional worker log\n\
         created: 2026-07-31T03:50:00Z\n\
         {updated}\
         node: fictional-node-a\n\
         agent: codex\n\
         session: {SESSION_ID}\n\
         request_id: {request_id}\n\
         status: active\n\
         ---\n\
         Fictional log body.\n"
    )
}

fn revision(contents: &str) -> Revision {
    Revision::from_bytes(Sha256::digest(contents.as_bytes()).into())
}

fn write_file(root: &Path, relative: &str, contents: &[u8]) {
    let path = root.join(relative);
    if let Some(parent) = path.parent()
        && let Err(error) = fs::create_dir_all(parent)
    {
        panic!("fixture parent must be created: {error}");
    }
    if let Err(error) = fs::write(path, contents) {
        panic!("fixture must be written: {error}");
    }
}

fn read_file(path: &Path) -> Vec<u8> {
    match fs::read(path) {
        Ok(contents) => contents,
        Err(error) => panic!("fixture must be readable: {error}"),
    }
}

fn package(root: &Path, request: Value, payload: &[(&str, &[u8])]) -> ValidatedPackage {
    if let Err(error) = fs::create_dir(root) {
        panic!("package root must be created: {error}");
    }
    if let Err(error) = fs::create_dir(root.join("payload")) {
        panic!("payload root must be created: {error}");
    }
    let encoded = match serde_json::to_vec_pretty(&request) {
        Ok(encoded) => encoded,
        Err(error) => panic!("request fixture must encode: {error}"),
    };
    write_file(root, "request.json", &encoded);
    for (path, contents) in payload {
        write_file(&root.join("payload"), path, contents);
    }
    match validate_package(root, &PackagePolicy::default()) {
        Ok(package) => package,
        Err(error) => panic!("request package fixture must validate: {error}"),
    }
}

fn request(
    request_id: &str,
    project: Option<&str>,
    document_type: &str,
    operations: Value,
) -> Value {
    let mut value = json!({
        "protocol_version": 1,
        "request_id": request_id,
        "title": "Apply a fictional repository change",
        "document_type": document_type,
        "created_at": "2026-07-31T04:00:00Z",
        "operations": operations,
    });
    if let Some(project) = project {
        value["project"] = json!(project);
    }
    value
}

#[test]
fn creates_a_canonical_document_bundle_with_an_attachment() {
    let root = TestDirectory::new();
    let content_root = root.path().join("content");
    if let Err(error) = fs::create_dir(&content_root) {
        panic!("content root must be created: {error}");
    }
    let markdown = document(DOCUMENT_ID, CREATE_REQUEST_ID, "Fictional experiment", None);
    let package_root = root.path().join("package");
    let package = package(
        &package_root,
        request(
            CREATE_REQUEST_ID,
            Some("fictional-project"),
            "experiment",
            json!([
                {
                    "type": "create_document",
                    "document_id": DOCUMENT_ID,
                    "content": "index.md"
                },
                {
                    "type": "add_attachment",
                    "document_id": DOCUMENT_ID,
                    "source": "results.csv",
                    "name": "results.csv"
                }
            ]),
        ),
        &[
            ("index.md", markdown.as_bytes()),
            ("results.csv", b"value\n42\n"),
        ],
    );

    let outcome = match apply_request(
        &content_root,
        &package_root,
        &package,
        ContentPolicy::default(),
    ) {
        Ok(outcome) => outcome,
        Err(error) => panic!("valid create request must apply: {error}"),
    };
    assert_eq!(outcome.operations_applied(), 2);
    let bundle = content_root.join(format!(
        "projects/fictional-project/experiments/2026-07-31-{DOCUMENT_ID}"
    ));
    assert_eq!(read_file(&bundle.join("index.md")), markdown.into_bytes());
    assert_eq!(read_file(&bundle.join("results.csv")), b"value\n42\n");
}

#[test]
fn update_requires_the_exact_revision_before_replacing_bytes() {
    let root = TestDirectory::new();
    let content_root = root.path().join("content");
    let relative = format!("projects/fictional-project/runbooks/2026-07-31-{DOCUMENT_ID}/index.md");
    let original = document(DOCUMENT_ID, ORIGINAL_REQUEST_ID, "Fictional runbook", None);
    write_file(&content_root, &relative, original.as_bytes());
    let replacement = document(
        DOCUMENT_ID,
        UPDATE_REQUEST_ID,
        "Updated fictional runbook",
        Some("2026-07-31T04:00:00Z"),
    );
    let package_root = root.path().join("conflict-package");
    let conflict = package(
        &package_root,
        request(
            UPDATE_REQUEST_ID,
            Some("fictional-project"),
            "runbook",
            json!([{
                "type": "update_document",
                "document_id": DOCUMENT_ID,
                "expected_revision": Revision::from_bytes([7; 32]).to_string(),
                "content": "index.md"
            }]),
        ),
        &[("index.md", replacement.as_bytes())],
    );
    assert!(matches!(
        apply_request(
            &content_root,
            &package_root,
            &conflict,
            ContentPolicy::default()
        ),
        Err(ApplyError::RevisionConflict { .. })
    ));
    assert_eq!(
        read_file(&content_root.join(&relative)),
        original.into_bytes()
    );

    let package_root = root.path().join("valid-package");
    let valid = package(
        &package_root,
        request(
            UPDATE_REQUEST_ID,
            Some("fictional-project"),
            "runbook",
            json!([{
                "type": "update_document",
                "document_id": DOCUMENT_ID,
                "expected_revision": revision(&document(
                    DOCUMENT_ID,
                    ORIGINAL_REQUEST_ID,
                    "Fictional runbook",
                    None,
                )).to_string(),
                "content": "index.md"
            }]),
        ),
        &[("index.md", replacement.as_bytes())],
    );
    if let Err(error) = apply_request(
        &content_root,
        &package_root,
        &valid,
        ContentPolicy::default(),
    ) {
        panic!("matching update must apply: {error}");
    }
    assert_eq!(
        read_file(&content_root.join(relative)),
        replacement.into_bytes()
    );
}

#[test]
fn update_preserves_immutable_creation_time() {
    let root = TestDirectory::new();
    let content_root = root.path().join("content");
    let relative = format!("projects/fictional-project/runbooks/2026-07-31-{DOCUMENT_ID}/index.md");
    let original = document(DOCUMENT_ID, ORIGINAL_REQUEST_ID, "Fictional runbook", None);
    write_file(&content_root, &relative, original.as_bytes());
    let replacement = document(
        DOCUMENT_ID,
        UPDATE_REQUEST_ID,
        "Retimed fictional runbook",
        Some("2026-07-31T04:00:00Z"),
    )
    .replace(
        "created: 2026-07-31T03:50:00Z",
        "created: 2026-07-30T03:50:00Z",
    );
    let package_root = root.path().join("package");
    let package = package(
        &package_root,
        request(
            UPDATE_REQUEST_ID,
            Some("fictional-project"),
            "runbook",
            json!([{
                "type": "update_document",
                "document_id": DOCUMENT_ID,
                "expected_revision": revision(&original).to_string(),
                "content": "index.md"
            }]),
        ),
        &[("index.md", replacement.as_bytes())],
    );
    assert!(matches!(
        apply_request(
            &content_root,
            &package_root,
            &package,
            ContentPolicy::default()
        ),
        Err(ApplyError::OperationForbidden { .. })
    ));
    assert_eq!(
        read_file(&content_root.join(relative)),
        original.into_bytes()
    );
}

#[test]
fn moves_and_archives_complete_document_bundles() {
    let root = TestDirectory::new();
    let content_root = root.path().join("content");
    let source = format!("projects/fictional-a/decisions/2026-07-31-{DOCUMENT_ID}");
    let markdown = document(DOCUMENT_ID, ORIGINAL_REQUEST_ID, "Fictional decision", None);
    write_file(
        &content_root,
        &format!("{source}/index.md"),
        markdown.as_bytes(),
    );
    write_file(
        &content_root,
        &format!("{source}/report.pdf"),
        b"fictional\n",
    );

    let move_root = root.path().join("move-package");
    let move_package = package(
        &move_root,
        request(
            MOVE_REQUEST_ID,
            Some("fictional-b"),
            "runbook",
            json!([{
                "type": "move_document",
                "document_id": DOCUMENT_ID,
                "expected_revision": revision(&markdown).to_string(),
                "project": "fictional-b",
                "document_type": "runbook"
            }]),
        ),
        &[],
    );
    if let Err(error) = apply_request(
        &content_root,
        &move_root,
        &move_package,
        ContentPolicy::default(),
    ) {
        panic!("valid move must apply: {error}");
    }
    let moved = format!("projects/fictional-b/runbooks/2026-07-31-{DOCUMENT_ID}");
    assert!(!content_root.join(source).exists());
    assert_eq!(
        read_file(&content_root.join(format!("{moved}/report.pdf"))),
        b"fictional\n"
    );

    let archive_root = root.path().join("archive-package");
    let archive_package = package(
        &archive_root,
        request(
            ARCHIVE_REQUEST_ID,
            Some("fictional-b"),
            "runbook",
            json!([{
                "type": "archive_document",
                "document_id": DOCUMENT_ID,
                "expected_revision": revision(&markdown).to_string()
            }]),
        ),
        &[],
    );
    if let Err(error) = apply_request(
        &content_root,
        &archive_root,
        &archive_package,
        ContentPolicy::default(),
    ) {
        panic!("valid archive must apply: {error}");
    }
    let archived = format!("projects/fictional-b/archive/runbooks/2026-07-31-{DOCUMENT_ID}");
    assert!(!content_root.join(moved).exists());
    assert_eq!(
        read_file(&content_root.join(format!("{archived}/report.pdf"))),
        b"fictional\n"
    );
    let archived_markdown = read_file(&content_root.join(format!("{archived}/index.md")));
    let metadata = match agent_knowledge_core::decode_document_metadata(
        &archived_markdown,
        ContentPolicy::default().maximum_front_matter_bytes,
    ) {
        Ok(metadata) => metadata,
        Err(error) => panic!("archived front matter must decode: {error}"),
    };
    assert_eq!(
        metadata.status,
        agent_knowledge_core::DocumentStatus::Archived
    );
    assert_eq!(metadata.request_id.to_string(), ARCHIVE_REQUEST_ID);
    assert!(metadata.updated.is_some());
}

#[test]
fn archives_a_document_created_in_the_same_request() {
    let root = TestDirectory::new();
    let content_root = root.path().join("content");
    if let Err(error) = fs::create_dir(&content_root) {
        panic!("content root must be created: {error}");
    }
    let markdown = document(
        DOCUMENT_ID,
        ARCHIVE_REQUEST_ID,
        "Ephemeral experiment",
        None,
    );
    let package_root = root.path().join("package");
    let package = package(
        &package_root,
        request(
            ARCHIVE_REQUEST_ID,
            Some("fictional-project"),
            "experiment",
            json!([
                {
                    "type": "create_document",
                    "document_id": DOCUMENT_ID,
                    "content": "index.md"
                },
                {
                    "type": "archive_document",
                    "document_id": DOCUMENT_ID,
                    "expected_revision": revision(&markdown).to_string()
                }
            ]),
        ),
        &[("index.md", markdown.as_bytes())],
    );

    let outcome = match apply_request(
        &content_root,
        &package_root,
        &package,
        ContentPolicy::default(),
    ) {
        Ok(outcome) => outcome,
        Err(error) => panic!("create followed by archive must apply atomically: {error}"),
    };
    assert_eq!(outcome.operations_applied(), 2);
    let archived = content_root.join(format!(
        "projects/fictional-project/archive/experiments/2026-07-31-{DOCUMENT_ID}/index.md"
    ));
    assert!(archived.is_file());
    assert_eq!(outcome.moves().len(), 1);
}

#[test]
fn archives_the_latest_update_from_the_same_request() {
    let root = TestDirectory::new();
    let content_root = root.path().join("content");
    let relative = format!("projects/fictional-project/runbooks/2026-07-31-{DOCUMENT_ID}/index.md");
    let original = document(DOCUMENT_ID, ORIGINAL_REQUEST_ID, "Original runbook", None);
    write_file(&content_root, &relative, original.as_bytes());
    let replacement = document(
        DOCUMENT_ID,
        ARCHIVE_REQUEST_ID,
        "Updated before archive",
        Some("2026-07-31T03:55:00Z"),
    );
    let package_root = root.path().join("package");
    let package = package(
        &package_root,
        request(
            ARCHIVE_REQUEST_ID,
            Some("fictional-project"),
            "runbook",
            json!([
                {
                    "type": "update_document",
                    "document_id": DOCUMENT_ID,
                    "expected_revision": revision(&original).to_string(),
                    "content": "index.md"
                },
                {
                    "type": "archive_document",
                    "document_id": DOCUMENT_ID,
                    "expected_revision": revision(&replacement).to_string()
                }
            ]),
        ),
        &[("index.md", replacement.as_bytes())],
    );

    if let Err(error) = apply_request(
        &content_root,
        &package_root,
        &package,
        ContentPolicy::default(),
    ) {
        panic!("update followed by archive must use the replacement bytes: {error}");
    }
    let archived = content_root.join(format!(
        "projects/fictional-project/archive/runbooks/2026-07-31-{DOCUMENT_ID}/index.md"
    ));
    let archived = String::from_utf8(read_file(&archived))
        .unwrap_or_else(|error| panic!("archived Markdown must be UTF-8: {error}"));
    assert!(archived.contains("Updated before archive"));
}

#[test]
fn archives_a_document_moved_in_the_same_request() {
    let root = TestDirectory::new();
    let content_root = root.path().join("content");
    let source = format!("projects/fictional-a/decisions/2026-07-31-{DOCUMENT_ID}");
    let markdown = document(DOCUMENT_ID, ORIGINAL_REQUEST_ID, "Movable decision", None);
    write_file(
        &content_root,
        &format!("{source}/index.md"),
        markdown.as_bytes(),
    );
    write_file(
        &content_root,
        &format!("{source}/evidence.pdf"),
        b"fictional evidence\n",
    );
    let package_root = root.path().join("package");
    let package = package(
        &package_root,
        request(
            ARCHIVE_REQUEST_ID,
            Some("fictional-b"),
            "runbook",
            json!([
                {
                    "type": "move_document",
                    "document_id": DOCUMENT_ID,
                    "expected_revision": revision(&markdown).to_string(),
                    "project": "fictional-b",
                    "document_type": "runbook"
                },
                {
                    "type": "archive_document",
                    "document_id": DOCUMENT_ID,
                    "expected_revision": revision(&markdown).to_string()
                }
            ]),
        ),
        &[],
    );

    let outcome = match apply_request(
        &content_root,
        &package_root,
        &package,
        ContentPolicy::default(),
    ) {
        Ok(outcome) => outcome,
        Err(error) => panic!("move followed by archive must use the virtual path: {error}"),
    };
    let archived = content_root.join(format!(
        "projects/fictional-b/archive/runbooks/2026-07-31-{DOCUMENT_ID}"
    ));
    assert_eq!(
        read_file(&archived.join("evidence.pdf")),
        b"fictional evidence\n"
    );
    assert_eq!(outcome.moves().len(), 2);
}

#[test]
fn rejects_document_timestamps_after_request_acceptance() {
    let root = TestDirectory::new();
    let content_root = root.path().join("content");
    if let Err(error) = fs::create_dir(&content_root) {
        panic!("content root must be created: {error}");
    }
    let future = document(DOCUMENT_ID, CREATE_REQUEST_ID, "Future experiment", None).replace(
        "created: 2026-07-31T03:50:00Z",
        "created: 2026-07-31T04:00:01Z",
    );
    let package_root = root.path().join("package");
    let package = package(
        &package_root,
        request(
            CREATE_REQUEST_ID,
            Some("fictional-project"),
            "experiment",
            json!([{
                "type": "create_document",
                "document_id": DOCUMENT_ID,
                "content": "index.md"
            }]),
        ),
        &[("index.md", future.as_bytes())],
    );
    assert!(matches!(
        apply_request(
            &content_root,
            &package_root,
            &package,
            ContentPolicy::default()
        ),
        Err(ApplyError::OperationForbidden { .. })
    ));
}

#[test]
fn append_only_logs_reject_updates() {
    let root = TestDirectory::new();
    let content_root = root.path().join("content");
    let relative =
        format!("projects/fictional-project/logs/2026/07/31/035000-{DOCUMENT_ID}/index.md");
    let original = log_document(ORIGINAL_REQUEST_ID, None);
    write_file(&content_root, &relative, original.as_bytes());
    let replacement = log_document(UPDATE_REQUEST_ID, Some("2026-07-31T04:00:00Z"));
    let package_root = root.path().join("package");
    let mut update_request = request(
        UPDATE_REQUEST_ID,
        Some("fictional-project"),
        "log",
        json!([{
            "type": "update_document",
            "document_id": DOCUMENT_ID,
            "expected_revision": revision(&original).to_string(),
            "content": "index.md"
        }]),
    );
    update_request["node"] = json!("fictional-node-a");
    update_request["agent"] = json!("codex");
    update_request["session"] = json!(SESSION_ID);
    let package = package(
        &package_root,
        update_request,
        &[("index.md", replacement.as_bytes())],
    );
    assert!(matches!(
        apply_request(
            &content_root,
            &package_root,
            &package,
            ContentPolicy::default()
        ),
        Err(ApplyError::OperationForbidden { .. })
    ));
    assert_eq!(
        read_file(&content_root.join(relative)),
        original.into_bytes()
    );
}

#[test]
fn deterministic_failure_is_detected_before_any_mutation() {
    let root = TestDirectory::new();
    let content_root = root.path().join("content");
    let bundle = format!("projects/fictional-project/runbooks/2026-07-31-{DOCUMENT_ID}");
    let original = document(DOCUMENT_ID, ORIGINAL_REQUEST_ID, "Fictional runbook", None);
    write_file(
        &content_root,
        &format!("{bundle}/index.md"),
        original.as_bytes(),
    );
    write_file(
        &content_root,
        &format!("{bundle}/report.pdf"),
        b"existing\n",
    );
    let replacement = document(
        DOCUMENT_ID,
        UPDATE_REQUEST_ID,
        "Updated fictional runbook",
        Some("2026-07-31T04:00:00Z"),
    );
    let package_root = root.path().join("package");
    let package = package(
        &package_root,
        request(
            UPDATE_REQUEST_ID,
            Some("fictional-project"),
            "runbook",
            json!([
                {
                    "type": "update_document",
                    "document_id": DOCUMENT_ID,
                    "expected_revision": revision(&original).to_string(),
                    "content": "index.md"
                },
                {
                    "type": "add_attachment",
                    "document_id": DOCUMENT_ID,
                    "source": "report.pdf",
                    "name": "report.pdf"
                }
            ]),
        ),
        &[
            ("index.md", replacement.as_bytes()),
            ("report.pdf", b"replacement\n"),
        ],
    );
    assert!(matches!(
        apply_request(
            &content_root,
            &package_root,
            &package,
            ContentPolicy::default()
        ),
        Err(ApplyError::DestinationExists { .. })
    ));
    assert_eq!(
        read_file(&content_root.join(format!("{bundle}/index.md"))),
        original.into_bytes()
    );
    assert_eq!(
        read_file(&content_root.join(format!("{bundle}/report.pdf"))),
        b"existing\n"
    );
}

#[test]
fn rejects_payload_bytes_changed_after_package_validation() {
    let root = TestDirectory::new();
    let content_root = root.path().join("content");
    if let Err(error) = fs::create_dir(&content_root) {
        panic!("content root must be created: {error}");
    }
    let markdown = document(DOCUMENT_ID, CREATE_REQUEST_ID, "Fictional experiment", None);
    let package_root = root.path().join("package");
    let package = package(
        &package_root,
        request(
            CREATE_REQUEST_ID,
            Some("fictional-project"),
            "experiment",
            json!([{
                "type": "create_document",
                "document_id": DOCUMENT_ID,
                "content": "index.md"
            }]),
        ),
        &[("index.md", markdown.as_bytes())],
    );
    let changed = markdown.replace("Fictional experiment", "Malicious experiment");
    assert_eq!(changed.len(), markdown.len());
    write_file(
        &package_root.join("payload"),
        "index.md",
        changed.as_bytes(),
    );
    assert!(matches!(
        apply_request(
            &content_root,
            &package_root,
            &package,
            ContentPolicy::default()
        ),
        Err(ApplyError::ClaimPackageChanged)
    ));
    assert!(
        !content_root
            .join(format!(
                "projects/fictional-project/experiments/2026-07-31-{DOCUMENT_ID}"
            ))
            .exists()
    );
}

#[test]
fn rejects_archived_creation_and_regressing_update_time() {
    let root = TestDirectory::new();
    let content_root = root.path().join("content");
    if let Err(error) = fs::create_dir(&content_root) {
        panic!("content root must be created: {error}");
    }
    let archived = document(DOCUMENT_ID, CREATE_REQUEST_ID, "Archived creation", None)
        .replace("status: active", "status: archived");
    let package_root = root.path().join("archived-package");
    let archived_package = package(
        &package_root,
        request(
            CREATE_REQUEST_ID,
            Some("fictional-project"),
            "experiment",
            json!([{
                "type": "create_document",
                "document_id": DOCUMENT_ID,
                "content": "index.md"
            }]),
        ),
        &[("index.md", archived.as_bytes())],
    );
    assert!(matches!(
        apply_request(
            &content_root,
            &package_root,
            &archived_package,
            ContentPolicy::default()
        ),
        Err(ApplyError::OperationForbidden { .. })
    ));

    let original = document(
        DOCUMENT_ID,
        ORIGINAL_REQUEST_ID,
        "Fictional runbook",
        Some("2026-07-31T05:00:00Z"),
    );
    let relative = format!("projects/fictional-project/runbooks/2026-07-31-{DOCUMENT_ID}/index.md");
    write_file(&content_root, &relative, original.as_bytes());
    let replacement = document(
        DOCUMENT_ID,
        UPDATE_REQUEST_ID,
        "Regressed fictional runbook",
        Some("2026-07-31T04:00:00Z"),
    );
    let package_root = root.path().join("regressed-package");
    let regressed = package(
        &package_root,
        request(
            UPDATE_REQUEST_ID,
            Some("fictional-project"),
            "runbook",
            json!([{
                "type": "update_document",
                "document_id": DOCUMENT_ID,
                "expected_revision": revision(&original).to_string(),
                "content": "index.md"
            }]),
        ),
        &[("index.md", replacement.as_bytes())],
    );
    assert!(matches!(
        apply_request(
            &content_root,
            &package_root,
            &regressed,
            ContentPolicy::default()
        ),
        Err(ApplyError::OperationForbidden { .. })
    ));
    assert_eq!(
        read_file(&content_root.join(relative)),
        original.into_bytes()
    );
}

#[test]
fn resulting_content_io_failure_remains_retryable() {
    let error = ApplyError::ResultingContent(crate::ContentIndexError::Io(io::Error::other(
        "fictional transient failure",
    )));
    assert_eq!(error.request_error_code(), None);
}

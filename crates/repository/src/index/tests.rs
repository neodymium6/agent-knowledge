use std::fs;
use std::path::{Path, PathBuf};

use agent_knowledge_core::{DocumentId, Revision};
use sha2::{Digest, Sha256};
use ulid::Ulid;

use super::{ContentIndex, ContentIndexError, ContentPolicy, RevisionCheckError};

const DOCUMENT_ID: &str = "01K00000000000000000000000";
const OTHER_DOCUMENT_ID: &str = "01K00000000000000000000001";
const REQUEST_ID: &str = "01K00000000000000000000002";

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "agent-knowledge-repository-index-test-{}",
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

fn markdown(document_id: &str, title: &str) -> String {
    format!(
        "---\n\
         schema_version: 1\n\
         document_id: {document_id}\n\
         title: {title}\n\
         created: 2026-07-31T03:50:00Z\n\
         request_id: {REQUEST_ID}\n\
         status: active\n\
         ---\n\
         Fictional body.\n"
    )
}

fn write_document(root: &Path, relative: &str, contents: &str) {
    let path = root.join(relative);
    if let Some(parent) = path.parent()
        && let Err(error) = fs::create_dir_all(parent)
    {
        panic!("document parent must be created: {error}");
    }
    if let Err(error) = fs::write(path, contents) {
        panic!("document fixture must be written: {error}");
    }
}

fn parse_document_id(value: &str) -> DocumentId {
    match value.parse() {
        Ok(value) => value,
        Err(error) => panic!("fixture document ID must parse: {error}"),
    }
}

#[test]
fn indexes_documents_and_exact_byte_revisions() {
    let root = TestDirectory::new();
    let contents = markdown(DOCUMENT_ID, "Fictional runbook");
    write_document(
        root.path(),
        "projects/fictional/runbooks/example/index.md",
        &contents,
    );
    write_document(
        root.path(),
        "projects/fictional/runbooks/example/report.json",
        "{}\n",
    );

    let index = match ContentIndex::build(root.path(), ContentPolicy::default()) {
        Ok(index) => index,
        Err(error) => panic!("valid content must index: {error}"),
    };
    assert_eq!(index.len(), 1);
    let document_id = parse_document_id(DOCUMENT_ID);
    let record = match index.get(document_id) {
        Some(record) => record,
        None => panic!("indexed document must resolve"),
    };
    assert_eq!(
        record.relative_path(),
        Path::new("projects/fictional/runbooks/example/index.md")
    );
    assert_eq!(record.metadata().title, "Fictional runbook");
    let revision = Revision::from_bytes(Sha256::digest(contents.as_bytes()).into());
    assert_eq!(record.revision(), revision);
    assert!(index.require_revision(document_id, revision).is_ok());
}

#[test]
fn optimistic_check_reports_missing_and_conflicting_documents() {
    let root = TestDirectory::new();
    let contents = markdown(DOCUMENT_ID, "Fictional decision");
    write_document(
        root.path(),
        "projects/fictional/decisions/example/index.md",
        &contents,
    );
    let index = match ContentIndex::build(root.path(), ContentPolicy::default()) {
        Ok(index) => index,
        Err(error) => panic!("valid content must index: {error}"),
    };
    let document_id = parse_document_id(DOCUMENT_ID);
    let wrong_revision = Revision::from_bytes([7; 32]);
    assert!(matches!(
        index.require_revision(document_id, wrong_revision),
        Err(RevisionCheckError::RevisionConflict {
            document_id: found,
            expected,
            ..
        }) if found == document_id && expected == wrong_revision
    ));

    let missing = parse_document_id(OTHER_DOCUMENT_ID);
    assert_eq!(
        index.require_revision(missing, wrong_revision),
        Err(RevisionCheckError::NotFound {
            document_id: missing
        })
    );
}

#[test]
fn duplicate_document_ids_fail_the_complete_index() {
    let root = TestDirectory::new();
    let contents = markdown(DOCUMENT_ID, "Fictional reference");
    write_document(
        root.path(),
        "projects/fictional/references/first/index.md",
        &contents,
    );
    write_document(
        root.path(),
        "projects/fictional/references/second/index.md",
        &contents,
    );

    assert!(matches!(
        ContentIndex::build(root.path(), ContentPolicy::default()),
        Err(ContentIndexError::DuplicateDocumentId {
            document_id,
            first_path,
            second_path,
        }) if document_id == parse_document_id(DOCUMENT_ID)
            && first_path == Path::new("projects/fictional/references/first/index.md")
            && second_path == Path::new("projects/fictional/references/second/index.md")
    ));
}

#[test]
fn rejects_unsafe_entries_and_enforces_bounds() {
    let root = TestDirectory::new();
    write_document(
        root.path(),
        "projects/fictional/references/example/index.md",
        &markdown(DOCUMENT_ID, "Fictional reference"),
    );
    write_document(root.path(), "attachment.json", "{}\n");
    assert!(matches!(
        ContentIndex::build(
            root.path(),
            ContentPolicy {
                maximum_entry_count: 1,
                ..ContentPolicy::default()
            }
        ),
        Err(ContentIndexError::EntryLimitExceeded { maximum: 1 })
    ));

    assert!(matches!(
        ContentIndex::build(
            root.path(),
            ContentPolicy {
                maximum_markdown_bytes: 8,
                ..ContentPolicy::default()
            }
        ),
        Err(ContentIndexError::MarkdownTooLarge { maximum: 8, .. })
    ));
}

#[cfg(unix)]
#[test]
fn rejects_symbolic_links() {
    use std::os::unix::fs::symlink;

    let root = TestDirectory::new();
    write_document(
        root.path(),
        "projects/fictional/references/example/index.md",
        &markdown(DOCUMENT_ID, "Fictional reference"),
    );
    let link = root.path().join("projects/fictional/references/link.md");
    if let Some(parent) = link.parent()
        && let Err(error) = fs::create_dir_all(parent)
    {
        panic!("symbolic-link parent must be created: {error}");
    }
    if let Err(error) = symlink("example.md", &link) {
        panic!("symbolic-link fixture must be created: {error}");
    }
    assert!(matches!(
        ContentIndex::build(root.path(), ContentPolicy::default()),
        Err(ContentIndexError::InvalidEntryType(path))
            if path == Path::new("projects/fictional/references/link.md")
    ));
}

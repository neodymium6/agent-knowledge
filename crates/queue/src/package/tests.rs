use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use agent_knowledge_core::ErrorCode;

use super::{PackageLimit, PackageLimits, PackagePolicy, PackageValidationError, validate_package};

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
        },
        {
            "type": "add_attachment",
            "document_id": "01K00000000000000000000002",
            "source": "benchmark/results.csv",
            "name": "results.csv"
        }
    ]
}"#;

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn create() -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "agent-knowledge-queue-test-{}-{sequence}",
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
            && error.kind() != std::io::ErrorKind::NotFound
        {
            panic!(
                "test directory must be removed at {}: {error}",
                self.0.display()
            );
        }
    }
}

fn write_fixture(root: &Path, request_json: &str, reverse_payload_order: bool) {
    let payload = root.join("payload/benchmark");
    if let Err(error) = fs::create_dir_all(&payload) {
        panic!("fixture payload directory must be created: {error}");
    }
    if let Err(error) = fs::write(root.join("request.json"), request_json) {
        panic!("fixture request must be written: {error}");
    }

    let files = [
        ("index.md", b"---\ntitle: Fictional\n---\n" as &[u8]),
        ("results.csv", b"step,value\n1,42\n" as &[u8]),
    ];
    let order: &[usize] = if reverse_payload_order {
        &[1, 0]
    } else {
        &[0, 1]
    };
    for index in order {
        let (name, contents) = files[*index];
        if let Err(error) = fs::write(payload.join(name), contents) {
            panic!("fixture payload must be written: {error}");
        }
    }
}

#[test]
fn validates_complete_package_and_calculates_stable_digest() {
    let first = TestDirectory::create();
    write_fixture(first.path(), REQUEST_JSON, false);
    let first_package = match validate_package(first.path(), &PackagePolicy::default()) {
        Ok(package) => package,
        Err(error) => panic!("first package must validate: {error}"),
    };

    let second = TestDirectory::create();
    let compact_request = match serde_json::from_str::<serde_json::Value>(REQUEST_JSON) {
        Ok(value) => value.to_string(),
        Err(error) => panic!("fixture JSON must parse: {error}"),
    };
    write_fixture(second.path(), &compact_request, true);
    let second_package = match validate_package(second.path(), &PackagePolicy::default()) {
        Ok(package) => package,
        Err(error) => panic!("second package must validate: {error}"),
    };

    assert_eq!(first_package.digest(), second_package.digest());
    assert_eq!(first_package.payload().len(), 2);
    assert_eq!(
        first_package.payload()[0].path().as_str(),
        "benchmark/index.md"
    );
}

#[test]
fn rejects_missing_and_unreferenced_payload() {
    let missing = TestDirectory::create();
    write_fixture(missing.path(), REQUEST_JSON, false);
    if let Err(error) = fs::remove_file(missing.path().join("payload/benchmark/index.md")) {
        panic!("fixture payload must be removed: {error}");
    }
    let error = match validate_package(missing.path(), &PackagePolicy::default()) {
        Ok(_) => panic!("missing payload must fail"),
        Err(error) => error,
    };
    assert!(matches!(error, PackageValidationError::MissingPayload(_)));

    let unexpected = TestDirectory::create();
    write_fixture(unexpected.path(), REQUEST_JSON, false);
    if let Err(error) = fs::write(
        unexpected.path().join("payload/benchmark/unexpected.json"),
        "{}",
    ) {
        panic!("unexpected fixture payload must be written: {error}");
    }
    let error = match validate_package(unexpected.path(), &PackagePolicy::default()) {
        Ok(_) => panic!("unexpected payload must fail"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        PackageValidationError::UnexpectedPayload(_)
    ));
}

#[test]
fn enforces_file_count_and_byte_limits() {
    let root = TestDirectory::create();
    write_fixture(root.path(), REQUEST_JSON, false);
    let policy = match PackagePolicy::new(
        PackageLimits {
            maximum_file_count: 2,
            ..PackageLimits::default()
        },
        ["csv"],
    ) {
        Ok(policy) => policy,
        Err(error) => panic!("test policy must be valid: {error}"),
    };

    let error = match validate_package(root.path(), &policy) {
        Ok(_) => panic!("file-count limit must fail"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        PackageValidationError::LimitExceeded {
            limit: PackageLimit::FileCount,
            maximum: 2,
            actual: 3
        }
    ));
    assert_eq!(error.error_code(), ErrorCode::LimitExceeded);
}

#[test]
fn rejects_disallowed_attachment_extensions() {
    let root = TestDirectory::create();
    write_fixture(root.path(), REQUEST_JSON, false);
    let policy = match PackagePolicy::new(PackageLimits::default(), ["json"]) {
        Ok(policy) => policy,
        Err(error) => panic!("test policy must be valid: {error}"),
    };

    let error = match validate_package(root.path(), &policy) {
        Ok(_) => panic!("disallowed attachment must fail"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        PackageValidationError::UnsupportedAttachment(ref name) if name == "results.csv"
    ));
    assert_eq!(error.error_code(), ErrorCode::UnsupportedFileType);
}

#[cfg(unix)]
#[test]
fn rejects_symbolic_links() {
    use std::os::unix::fs::symlink;

    let root = TestDirectory::create();
    write_fixture(root.path(), REQUEST_JSON, false);
    let target = root.path().join("payload/benchmark/results.csv");
    let link = root.path().join("payload/benchmark/link.csv");
    if let Err(error) = symlink(target, link) {
        panic!("fixture symbolic link must be created: {error}");
    }

    let error = match validate_package(root.path(), &PackagePolicy::default()) {
        Ok(_) => panic!("symbolic link must fail"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        PackageValidationError::InvalidEntryType { .. }
    ));
}

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use agent_knowledge_core::ErrorCode;

use super::{
    MarkdownValidationError, PackageLimit, PackageLimits, PackagePolicy, PackageValidationError,
    validate_accepted_package, validate_package,
};

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

const ARCHIVE_REQUEST_JSON: &str = r#"{
    "protocol_version": 1,
    "request_id": "01K00000000000000000000003",
    "title": "Archive fictional benchmark",
    "project": "fictional-solver",
    "document_type": "experiment",
    "node": "fictional-node-a",
    "agent": "codex",
    "session": "01K00000000000000000000004",
    "created_at": "2026-07-31T04:00:00+09:00",
    "operations": [
        {
            "type": "archive_document",
            "document_id": "01K00000000000000000000002",
            "expected_revision": "sha256:0000000000000000000000000000000000000000000000000000000000000000"
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
        (
            "index.md",
            b"---\nschema_version: 1\ndocument_id: 01K00000000000000000000002\ntitle: Fictional benchmark\ncreated: 2026-07-31T03:50:00+09:00\nrequest_id: 01K00000000000000000000000\ntags:\n  - benchmark\nstatus: active\n---\n"
                as &[u8],
        ),
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

fn write_accepted_metadata(root: &Path, digest: super::PackageDigest, sequence: u64) {
    if let Err(error) = fs::write(root.join("digest"), format!("{digest}\n")) {
        panic!("fixture digest must be written: {error}");
    }
    let acceptance =
        format!("{{\"sequence\":{sequence},\"accepted_at\":\"2026-07-31T00:00:00Z\"}}\n");
    if let Err(error) = fs::write(root.join("acceptance.json"), acceptance) {
        panic!("fixture acceptance metadata must be written: {error}");
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
fn bounds_payload_bytes_materialized_by_repeated_references() {
    let root = TestDirectory::create();
    let mut request: serde_json::Value = serde_json::from_str(REQUEST_JSON)
        .unwrap_or_else(|error| panic!("fixture request must decode: {error}"));
    let operations = request["operations"]
        .as_array_mut()
        .unwrap_or_else(|| panic!("fixture operations must be an array"));
    for index in 0..64 {
        operations.push(serde_json::json!({
            "type": "add_attachment",
            "document_id": "01K00000000000000000000002",
            "source": "benchmark/results.csv",
            "name": format!("copy-{index}.csv")
        }));
    }
    write_fixture(root.path(), &request.to_string(), false);
    if let Err(error) = fs::write(
        root.path().join("payload/benchmark/results.csv"),
        vec![b'x'; 1024 * 1024],
    ) {
        panic!("large repeated payload fixture must be written: {error}");
    }
    assert!(matches!(
        validate_package(root.path(), &PackagePolicy::default()),
        Err(PackageValidationError::LimitExceeded {
            limit: PackageLimit::MaterializedBytes,
            ..
        })
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
fn bounds_payload_directory_count_and_path_depth() {
    let directories = TestDirectory::create();
    write_fixture(directories.path(), REQUEST_JSON, false);
    let policy = match PackagePolicy::new(
        PackageLimits {
            maximum_directory_count: 0,
            ..PackageLimits::default()
        },
        ["csv"],
    ) {
        Ok(policy) => policy,
        Err(error) => panic!("test policy must be valid: {error}"),
    };
    let error = match validate_package(directories.path(), &policy) {
        Ok(_) => panic!("directory-count limit must fail"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        PackageValidationError::LimitExceeded {
            limit: PackageLimit::DirectoryCount,
            maximum: 0,
            actual: 1
        }
    ));

    let entries = TestDirectory::create();
    write_fixture(entries.path(), REQUEST_JSON, false);
    let policy = match PackagePolicy::new(
        PackageLimits {
            maximum_entry_count: 2,
            ..PackageLimits::default()
        },
        ["csv"],
    ) {
        Ok(policy) => policy,
        Err(error) => panic!("test policy must be valid: {error}"),
    };
    let error = match validate_package(entries.path(), &policy) {
        Ok(_) => panic!("entry-count limit must fail"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        PackageValidationError::LimitExceeded {
            limit: PackageLimit::EntryCount,
            maximum: 2,
            ..
        }
    ));

    let depth = TestDirectory::create();
    write_fixture(depth.path(), REQUEST_JSON, false);
    let policy = match PackagePolicy::new(
        PackageLimits {
            maximum_path_components: 1,
            ..PackageLimits::default()
        },
        ["csv"],
    ) {
        Ok(policy) => policy,
        Err(error) => panic!("test policy must be valid: {error}"),
    };
    let error = match validate_package(depth.path(), &policy) {
        Ok(_) => panic!("path-component limit must fail"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        PackageValidationError::LimitExceeded {
            limit: PackageLimit::PathComponents,
            maximum: 1,
            actual: 2
        }
    ));
}

#[test]
fn counts_request_json_against_the_entry_limit_without_payload_files() {
    let root = TestDirectory::create();
    if let Err(error) = fs::create_dir(root.path().join("payload")) {
        panic!("empty payload directory must be created: {error}");
    }
    if let Err(error) = fs::write(root.path().join("request.json"), ARCHIVE_REQUEST_JSON) {
        panic!("archive request fixture must be written: {error}");
    }
    let policy = match PackagePolicy::new(
        PackageLimits {
            maximum_entry_count: 0,
            ..PackageLimits::default()
        },
        ["csv"],
    ) {
        Ok(policy) => policy,
        Err(error) => panic!("test policy must be valid: {error}"),
    };

    let error = match validate_package(root.path(), &policy) {
        Ok(_) => panic!("request.json must count toward the entry limit"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        PackageValidationError::LimitExceeded {
            limit: PackageLimit::EntryCount,
            maximum: 0,
            actual: 1
        }
    ));
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

#[test]
fn classifies_protocol_and_typed_path_failures() {
    let protocol = TestDirectory::create();
    let unsupported = REQUEST_JSON.replace("\"protocol_version\": 1", "\"protocol_version\": 2");
    write_fixture(protocol.path(), &unsupported, false);
    let error = match validate_package(protocol.path(), &PackagePolicy::default()) {
        Ok(_) => panic!("unsupported protocol must fail"),
        Err(error) => error,
    };
    assert_eq!(error.error_code(), ErrorCode::InvalidProtocol);

    let path = TestDirectory::create();
    let traversal = REQUEST_JSON.replace(
        "\"content\": \"benchmark/index.md\"",
        "\"content\": \"../index.md\"",
    );
    write_fixture(path.path(), &traversal, false);
    let error = match validate_package(path.path(), &PackagePolicy::default()) {
        Ok(_) => panic!("traversal path must fail"),
        Err(error) => error,
    };
    assert_eq!(error.error_code(), ErrorCode::InvalidPath);
}

#[test]
fn rejects_incomplete_or_inconsistent_front_matter() {
    let incomplete = TestDirectory::create();
    write_fixture(incomplete.path(), REQUEST_JSON, false);
    if let Err(error) = fs::write(
        incomplete.path().join("payload/benchmark/index.md"),
        "---\ntitle: Incomplete\n---\n",
    ) {
        panic!("incomplete fixture Markdown must be written: {error}");
    }
    let error = match validate_package(incomplete.path(), &PackagePolicy::default()) {
        Ok(_) => panic!("incomplete front matter must fail"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        PackageValidationError::InvalidFrontMatter(_)
    ));
    assert_eq!(error.error_code(), ErrorCode::InvalidFrontMatter);

    let mismatched = TestDirectory::create();
    write_fixture(mismatched.path(), REQUEST_JSON, false);
    let markdown_path = mismatched.path().join("payload/benchmark/index.md");
    let markdown = match fs::read_to_string(&markdown_path) {
        Ok(markdown) => markdown,
        Err(error) => panic!("fixture Markdown must be readable: {error}"),
    }
    .replace("01K00000000000000000000002", "01K00000000000000000000003");
    if let Err(error) = fs::write(markdown_path, markdown) {
        panic!("mismatched fixture Markdown must be written: {error}");
    }
    assert!(matches!(
        validate_package(mismatched.path(), &PackagePolicy::default()),
        Err(PackageValidationError::InvalidFrontMatter(_))
    ));

    let duplicate = TestDirectory::create();
    write_fixture(duplicate.path(), REQUEST_JSON, false);
    let markdown_path = duplicate.path().join("payload/benchmark/index.md");
    let markdown = match fs::read_to_string(&markdown_path) {
        Ok(markdown) => markdown,
        Err(error) => panic!("fixture Markdown must be readable: {error}"),
    }
    .replace(
        "title: Fictional benchmark",
        "title: Fictional benchmark\ntitle: Duplicate",
    );
    if let Err(error) = fs::write(markdown_path, markdown) {
        panic!("duplicate-key fixture Markdown must be written: {error}");
    }
    assert!(matches!(
        validate_package(duplicate.path(), &PackagePolicy::default()),
        Err(PackageValidationError::InvalidFrontMatter(_))
    ));
}

#[test]
fn limits_front_matter_before_yaml_deserialization() {
    let oversized = TestDirectory::create();
    write_fixture(oversized.path(), REQUEST_JSON, false);
    let policy = match PackagePolicy::new(
        PackageLimits {
            maximum_front_matter_bytes: 32,
            ..PackageLimits::default()
        },
        ["csv"],
    ) {
        Ok(policy) => policy,
        Err(error) => panic!("test policy must be valid: {error}"),
    };
    let error = match validate_package(oversized.path(), &policy) {
        Ok(_) => panic!("oversized front matter must fail"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        PackageValidationError::InvalidFrontMatter(MarkdownValidationError::FrontMatterTooLarge {
            maximum: 32,
            ..
        })
    ));

    let aliases = TestDirectory::create();
    write_fixture(aliases.path(), REQUEST_JSON, false);
    let markdown_path = aliases.path().join("payload/benchmark/index.md");
    let markdown = match fs::read_to_string(&markdown_path) {
        Ok(markdown) => markdown,
        Err(error) => panic!("fixture Markdown must be readable: {error}"),
    }
    .replace(
        "title: Fictional benchmark",
        "title: &fictional_title Fictional benchmark",
    )
    .replace("  - benchmark", "  - *fictional_title");
    if let Err(error) = fs::write(markdown_path, markdown) {
        panic!("alias fixture Markdown must be written: {error}");
    }
    assert!(matches!(
        validate_package(aliases.path(), &PackagePolicy::default()),
        Err(PackageValidationError::InvalidFrontMatter(
            MarkdownValidationError::InvalidYaml { .. }
        ))
    ));
}

#[test]
fn updated_documents_require_updated_metadata() {
    let root = TestDirectory::create();
    let update_request = REQUEST_JSON.replace(
        "\"type\": \"create_document\",",
        concat!(
            "\"type\": \"update_document\",",
            "\n                \"expected_revision\": ",
            "\"sha256:0000000000000000000000000000000000000000000000000000000000000000\","
        ),
    );
    write_fixture(root.path(), &update_request, false);

    let error = match validate_package(root.path(), &PackagePolicy::default()) {
        Ok(_) => panic!("updated document without `updated` metadata must fail"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        PackageValidationError::InvalidFrontMatter(_)
    ));
}

#[test]
fn parses_each_referenced_markdown_payload_once() {
    let root = TestDirectory::create();
    let mut request = match serde_json::from_str::<serde_json::Value>(REQUEST_JSON) {
        Ok(request) => request,
        Err(error) => panic!("fixture request JSON must parse: {error}"),
    };
    let operations = match request
        .get_mut("operations")
        .and_then(serde_json::Value::as_array_mut)
    {
        Some(operations) => operations,
        None => panic!("fixture operations must be an array"),
    };
    let update = serde_json::json!({
        "type": "update_document",
        "document_id": "01K00000000000000000000002",
        "expected_revision":
            "sha256:0000000000000000000000000000000000000000000000000000000000000000",
        "content": "benchmark/index.md"
    });
    operations[0] = update.clone();
    operations.insert(1, update);
    let request = match serde_json::to_string(&request) {
        Ok(request) => request,
        Err(error) => panic!("fixture request JSON must serialize: {error}"),
    };
    write_fixture(root.path(), &request, false);
    let markdown_path = root.path().join("payload/benchmark/index.md");
    let markdown = match fs::read_to_string(&markdown_path) {
        Ok(markdown) => markdown,
        Err(error) => panic!("fixture Markdown must be readable: {error}"),
    }
    .replace(
        "created: 2026-07-31T03:50:00+09:00",
        "created: 2026-07-31T03:50:00+09:00\nupdated: 2026-07-31T04:00:00+09:00",
    );
    if let Err(error) = fs::write(markdown_path, markdown) {
        panic!("updated fixture Markdown must be written: {error}");
    }

    super::markdown::reset_document_parse_count();
    if let Err(error) = validate_package(root.path(), &PackagePolicy::default()) {
        panic!("repeated Markdown reference must validate: {error}");
    }
    assert_eq!(super::markdown::document_parse_count(), 1);
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

#[cfg(unix)]
#[test]
fn rejects_hard_links_and_executable_files() {
    use std::os::unix::fs::PermissionsExt;

    let linked = TestDirectory::create();
    write_fixture(linked.path(), REQUEST_JSON, false);
    let target = linked.path().join("payload/benchmark/results.csv");
    if let Err(error) = fs::hard_link(
        &target,
        linked.path().join("payload/benchmark/hard-link.csv"),
    ) {
        panic!("fixture hard link must be created: {error}");
    }
    assert!(matches!(
        validate_package(linked.path(), &PackagePolicy::default()),
        Err(PackageValidationError::HardLinkedFile { .. })
    ));

    let executable = TestDirectory::create();
    write_fixture(executable.path(), REQUEST_JSON, false);
    let target = executable.path().join("payload/benchmark/results.csv");
    let mut permissions = match fs::metadata(&target) {
        Ok(metadata) => metadata.permissions(),
        Err(error) => panic!("fixture metadata must be readable: {error}"),
    };
    permissions.set_mode(0o755);
    if let Err(error) = fs::set_permissions(&target, permissions) {
        panic!("fixture executable mode must be set: {error}");
    }
    assert!(matches!(
        validate_package(executable.path(), &PackagePolicy::default()),
        Err(PackageValidationError::ExecutableFile { .. })
    ));
}

#[test]
fn revalidates_accepted_digest_and_detects_changed_contents() {
    let root = TestDirectory::create();
    write_fixture(root.path(), REQUEST_JSON, false);
    let package = match validate_package(root.path(), &PackagePolicy::default()) {
        Ok(package) => package,
        Err(error) => panic!("incoming fixture package must validate: {error}"),
    };
    write_accepted_metadata(root.path(), package.digest(), 1);

    let accepted = match validate_accepted_package(root.path(), &PackagePolicy::default()) {
        Ok(package) => package,
        Err(error) => panic!("accepted fixture package must validate: {error}"),
    };
    assert_eq!(accepted.digest(), package.digest());
    assert!(validate_package(root.path(), &PackagePolicy::default()).is_err());

    if let Err(error) = fs::write(
        root.path().join("payload/benchmark/results.csv"),
        "step,value\n1,99\n",
    ) {
        panic!("fixture payload must be changed: {error}");
    }
    let error = match validate_accepted_package(root.path(), &PackagePolicy::default()) {
        Ok(_) => panic!("changed accepted package must fail"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        PackageValidationError::StoredDigestMismatch { .. }
    ));
    assert_eq!(error.error_code(), ErrorCode::ContentValidationFailed);
}

#[test]
fn rejects_zero_accepted_sequence() {
    let root = TestDirectory::create();
    write_fixture(root.path(), REQUEST_JSON, false);
    let package = match validate_package(root.path(), &PackagePolicy::default()) {
        Ok(package) => package,
        Err(error) => panic!("incoming fixture package must validate: {error}"),
    };
    write_accepted_metadata(root.path(), package.digest(), 0);

    let error = match validate_accepted_package(root.path(), &PackagePolicy::default()) {
        Ok(_) => panic!("accepted sequence zero must fail"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        PackageValidationError::InvalidAcceptanceMetadata(_)
    ));
    assert_eq!(error.error_code(), ErrorCode::ContentValidationFailed);
}

#[test]
fn accepted_packages_allow_only_defined_worker_sidecars() {
    let root = TestDirectory::create();
    write_fixture(root.path(), REQUEST_JSON, false);
    let package = match validate_package(root.path(), &PackagePolicy::default()) {
        Ok(package) => package,
        Err(error) => panic!("incoming fixture package must validate: {error}"),
    };
    write_accepted_metadata(root.path(), package.digest(), 1);
    for sidecar in ["phase.json", "result.json"] {
        if let Err(error) = fs::write(root.path().join(sidecar), "{}\n") {
            panic!("fixture sidecar must be written: {error}");
        }
    }
    if let Err(error) = validate_accepted_package(root.path(), &PackagePolicy::default()) {
        panic!("defined Worker sidecars must validate: {error}");
    }

    if let Err(error) = fs::write(root.path().join("worker-note.json"), "{}\n") {
        panic!("unknown sidecar fixture must be written: {error}");
    }
    assert!(matches!(
        validate_accepted_package(root.path(), &PackagePolicy::default()),
        Err(PackageValidationError::UnexpectedTopLevelEntry(name))
            if name == "worker-note.json"
    ));
}

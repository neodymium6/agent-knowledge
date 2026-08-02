use std::ffi::OsStr;
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use agent_knowledge_core::PinnedDirectory;
use agent_knowledge_protocol::{
    ExportRequest, GetRequest, LIST_COMMAND, ListRequest, ListResponse, ProtocolErrorResponse,
    ReadFilterRequest, StatusRequest, SubmitResponse,
};
use agent_knowledge_queue::{PackagePolicy, validate_package};
use tar::Archive;

use super::{
    ClientCommandError, ControlOperation, MAXIMUM_CONTROL_RESPONSE_BYTES, MAXIMUM_RESPONSE_BYTES,
    PreparedPackage, control_with_program, decode_protocol_version, export_with_program,
    get_with_program, open_payload, read_bounded_diagnostic, read_bounded_response,
    status_with_program, submit_with_program,
};

const REQUEST_JSON: &str = r#"{
  "protocol_version": 1,
  "request_id": "01K00000000000000000000000",
  "title": "Record a fictional client test",
  "project": "fictional-project",
  "document_type": "experiment",
  "created_at": "2026-07-31T03:50:00Z",
  "operations": [{
    "type": "create_document",
    "document_id": "01K00000000000000000000001",
    "content": "run/index.md"
  }]
}"#;
const MARKDOWN: &str = "---\n\
schema_version: 1\n\
document_id: 01K00000000000000000000001\n\
title: Fictional client test\n\
created: 2026-07-31T03:50:00Z\n\
request_id: 01K00000000000000000000000\n\
status: active\n\
---\n\
Fictional client body.\n";
static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn create() -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "agent-knowledge-client-test-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path)
            .unwrap_or_else(|error| panic!("client test directory must be created: {error}"));
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.0)
            && error.kind() != io::ErrorKind::NotFound
        {
            panic!("client test directory must be removed: {error}");
        }
    }
}

fn write_package(root: &Path) -> PathBuf {
    let package = root.join("package");
    fs::create_dir_all(package.join("payload/run"))
        .unwrap_or_else(|error| panic!("package fixture must be created: {error}"));
    fs::write(package.join("request.json"), REQUEST_JSON)
        .unwrap_or_else(|error| panic!("request fixture must be written: {error}"));
    fs::write(package.join("payload/run/index.md"), MARKDOWN)
        .unwrap_or_else(|error| panic!("payload fixture must be written: {error}"));
    package
}

fn response(package: &Path) -> String {
    let validated = validate_package(package, &PackagePolicy::default())
        .unwrap_or_else(|error| panic!("response package must validate: {error}"));
    format!(
        "{{\"protocol_version\":1,\"status\":\"accepted\",\"request_id\":\"{}\",\"digest\":\"{}\"}}",
        validated.request().request_id,
        validated.digest()
    )
}

#[cfg(unix)]
fn write_program(path: &Path, script: &str) {
    use std::os::unix::fs::PermissionsExt;

    fs::write(path, script)
        .unwrap_or_else(|error| panic!("fake ssh program must be written: {error}"));
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .unwrap_or_else(|error| panic!("fake ssh program must be executable: {error}"));
}

#[cfg(unix)]
#[test]
fn invokes_system_ssh_without_a_shell_and_streams_a_valid_archive() {
    let root = TestDirectory::create();
    let package = write_package(root.path());
    let response = response(&package);
    let arguments = root.path().join("arguments");
    let archive = root.path().join("archive.tar");
    let program = root.path().join("fictional-ssh");
    let script = format!(
        "#!/bin/sh\nset -eu\nprintf '%s\\n' \"$@\" > '{}'\ncat > '{}'\nprintf '%s\\n' '{}'\n",
        arguments.display(),
        archive.display(),
        response
    );
    write_program(&program, &script);

    let mut output = Vec::new();
    let mut diagnostic = Vec::new();
    submit_with_program(
        program.as_os_str(),
        OsStr::new("-fictional-alias"),
        &package,
        Duration::from_secs(5),
        &mut output,
        &mut diagnostic,
    )
    .unwrap_or_else(|error| panic!("client submission must succeed: {error}"));

    let invoked = fs::read_to_string(arguments)
        .unwrap_or_else(|error| panic!("fake ssh arguments must be readable: {error}"));
    assert_eq!(
        invoked,
        "-T\n-o\nBatchMode=yes\n-o\nClearAllForwardings=yes\n-o\nForwardAgent=no\n-o\nForwardX11=no\n-o\nStdinNull=no\n-o\nForkAfterAuthentication=no\n--\n-fictional-alias\nakp-v1 submit\n"
    );
    assert_eq!(String::from_utf8_lossy(&output), format!("{response}\n"));
    assert!(diagnostic.is_empty());

    let bytes = fs::read(archive)
        .unwrap_or_else(|error| panic!("streamed archive must be readable: {error}"));
    let mut archive = Archive::new(bytes.as_slice());
    let entries = archive
        .entries()
        .unwrap_or_else(|error| panic!("streamed archive must be valid: {error}"));
    let mut paths = Vec::new();
    let mut markdown = None;
    for entry in entries {
        let mut entry = entry.unwrap_or_else(|error| panic!("archive entry must decode: {error}"));
        let path = entry
            .path()
            .unwrap_or_else(|error| panic!("archive path must decode: {error}"))
            .into_owned();
        if path == Path::new("payload/run/index.md") {
            let mut body = String::new();
            entry
                .read_to_string(&mut body)
                .unwrap_or_else(|error| panic!("payload entry must be readable: {error}"));
            markdown = Some(body);
        }
        paths.push(path);
    }
    assert_eq!(
        paths,
        [
            PathBuf::from("request.json"),
            PathBuf::from("payload"),
            PathBuf::from("payload/run/index.md")
        ]
    );
    assert_eq!(markdown.as_deref(), Some(MARKDOWN));
}

#[cfg(unix)]
#[test]
fn sends_typed_control_json_and_validates_the_response() {
    let root = TestDirectory::create();
    let arguments = root.path().join("control-arguments");
    let request_file = root.path().join("control-request.json");
    let program = root.path().join("fictional-control-ssh");
    let response = "{\"protocol_version\":1,\"commit\":\"0123456789abcdef0123456789abcdef01234567\",\"documents\":[]}";
    let script = format!(
        "#!/bin/sh\nset -eu\nprintf '%s\\n' \"$@\" > '{}'\ncat > '{}'\nprintf '%s\\n' '{}'\n",
        arguments.display(),
        request_file.display(),
        response,
    );
    write_program(&program, &script);
    let request = ListRequest::new(ReadFilterRequest::default(), 25);
    let mut output = Vec::new();
    control_with_program::<_, ListResponse>(
        program.as_os_str(),
        OsStr::new("fictional-alias"),
        ControlOperation::new(LIST_COMMAND, MAXIMUM_CONTROL_RESPONSE_BYTES),
        &request,
        Duration::from_secs(5),
        &mut output,
        Vec::new(),
    )
    .unwrap_or_else(|error| panic!("client control operation must succeed: {error}"));

    assert_eq!(
        fs::read_to_string(arguments)
            .unwrap_or_else(|error| panic!("control arguments must be readable: {error}")),
        "-T\n-o\nBatchMode=yes\n-o\nClearAllForwardings=yes\n-o\nForwardAgent=no\n-o\nForwardX11=no\n-o\nStdinNull=no\n-o\nForkAfterAuthentication=no\n--\nfictional-alias\nakp-v1 list\n"
    );
    let sent: ListRequest = serde_json::from_slice(
        &fs::read(request_file)
            .unwrap_or_else(|error| panic!("control request must be readable: {error}")),
    )
    .unwrap_or_else(|error| panic!("control request must decode: {error}"));
    assert_eq!(sent, request);
    assert_eq!(String::from_utf8_lossy(&output), format!("{response}\n"));
}

#[cfg(unix)]
#[test]
fn exports_a_validated_document_bundle_without_a_shell() {
    let root = TestDirectory::create();
    let document_id = "01K00000000000000000000001"
        .parse()
        .unwrap_or_else(|error| panic!("document ID fixture must parse: {error}"));
    let archive_path = root.path().join("bundle.tar");
    let request_path = root.path().join("export-request.json");
    let archive = export_archive("01K00000000000000000000001");
    fs::write(&archive_path, &archive)
        .unwrap_or_else(|error| panic!("export fixture must be written: {error}"));
    let program = root.path().join("fictional-export-ssh");
    let script = format!(
        "#!/bin/sh\nset -eu\ncat > '{}'\ncat '{}'\n",
        request_path.display(),
        archive_path.display(),
    );
    write_program(&program, &script);

    let mut output = Vec::new();
    export_with_program(
        program.as_os_str(),
        OsStr::new("fictional-alias"),
        &ExportRequest::new(document_id),
        Duration::from_secs(5),
        &mut output,
        Vec::new(),
    )
    .unwrap_or_else(|error| panic!("client export must succeed: {error}"));
    assert_eq!(output, archive);
    let sent: ExportRequest = serde_json::from_slice(
        &fs::read(request_path)
            .unwrap_or_else(|error| panic!("export request must be readable: {error}")),
    )
    .unwrap_or_else(|error| panic!("export request must decode: {error}"));
    assert_eq!(sent, ExportRequest::new(document_id));
}

#[cfg(unix)]
#[test]
fn rejects_an_export_for_a_different_document() {
    let root = TestDirectory::create();
    let requested = "01K00000000000000000000001"
        .parse()
        .unwrap_or_else(|error| panic!("document ID fixture must parse: {error}"));
    let archive_path = root.path().join("mismatched-bundle.tar");
    fs::write(&archive_path, export_archive("01K00000000000000000000009"))
        .unwrap_or_else(|error| panic!("export fixture must be written: {error}"));
    let program = root.path().join("fictional-mismatched-export-ssh");
    write_program(
        &program,
        &format!(
            "#!/bin/sh\nset -eu\ncat > /dev/null\ncat '{}'\n",
            archive_path.display()
        ),
    );
    assert!(matches!(
        export_with_program(
            program.as_os_str(),
            OsStr::new("fictional-alias"),
            &ExportRequest::new(requested),
            Duration::from_secs(5),
            Vec::new(),
            Vec::new(),
        ),
        Err(ClientCommandError::ExportDocumentMismatch)
    ));
}

fn export_archive(document_id: &str) -> Vec<u8> {
    let markdown = format!(
        "---\nschema_version: 1\ndocument_id: {document_id}\ntitle: Fictional export\ncreated: 2026-07-31T03:50:00Z\nrequest_id: 01K00000000000000000000000\nstatus: active\n---\nFictional export body.\n"
    );
    let mut builder = tar::Builder::new(Vec::new());
    for (path, bytes) in [
        ("index.md", markdown.as_bytes()),
        ("result.json", b"{\"fictional\":true}\n".as_slice()),
    ] {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Regular);
        header.set_mode(0o644);
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(0);
        header.set_size(bytes.len() as u64);
        header.set_cksum();
        builder
            .append_data(&mut header, path, bytes)
            .unwrap_or_else(|error| panic!("export entry must append: {error}"));
    }
    builder
        .into_inner()
        .unwrap_or_else(|error| panic!("export archive must finish: {error}"))
}

#[cfg(unix)]
#[test]
fn rejects_a_get_response_for_a_different_document() {
    let root = TestDirectory::create();
    let program = root.path().join("fictional-get-ssh");
    write_program(
        &program,
        "#!/bin/sh\ncat > /dev/null\nprintf '%s\\n' '{\"protocol_version\":1,\"commit\":\"0123456789abcdef0123456789abcdef01234567\",\"document\":{\"summary\":{\"path\":\"projects/fictional-project/runbooks/fictional/index.md\",\"document_type\":\"runbook\",\"project\":\"fictional-project\",\"archived\":false,\"revision\":\"sha256:0000000000000000000000000000000000000000000000000000000000000000\",\"metadata\":{\"schema_version\":1,\"document_id\":\"01K00000000000000000000002\",\"title\":\"Fictional response\",\"created\":\"2026-07-31T03:50:00Z\",\"request_id\":\"01K00000000000000000000003\",\"status\":\"active\"}},\"markdown\":\"---\\n---\\n\"}}'\n",
    );
    let requested = "01K00000000000000000000001"
        .parse()
        .unwrap_or_else(|error| panic!("requested document ID must parse: {error}"));
    assert!(matches!(
        get_with_program(
            program.as_os_str(),
            OsStr::new("fictional-alias"),
            &GetRequest::new(requested),
            Duration::from_secs(5),
            Vec::new(),
            Vec::new(),
        ),
        Err(ClientCommandError::DocumentResponseMismatch)
    ));
}

#[cfg(unix)]
#[test]
fn sends_status_request_and_rejects_a_mismatched_response() {
    let root = TestDirectory::create();
    let arguments = root.path().join("status-arguments");
    let request_file = root.path().join("status-request.json");
    let program = root.path().join("fictional-status-ssh");
    let script = format!(
        "#!/bin/sh\nset -eu\nprintf '%s\\n' \"$@\" > '{}'\ncat > '{}'\nprintf '%s\\n' '{{\"status\":\"pending\",\"protocol_version\":1,\"request_id\":\"01K00000000000000000000099\"}}'\n",
        arguments.display(),
        request_file.display(),
    );
    write_program(&program, &script);
    let request_id = "01K00000000000000000000000"
        .parse()
        .unwrap_or_else(|error| panic!("request ID fixture must parse: {error}"));
    assert!(matches!(
        status_with_program(
            program.as_os_str(),
            OsStr::new("fictional-alias"),
            &StatusRequest::new(request_id),
            Duration::from_secs(5),
            Vec::new(),
            Vec::new(),
        ),
        Err(ClientCommandError::RequestStatusResponseMismatch)
    ));
    assert_eq!(
        fs::read_to_string(arguments)
            .unwrap_or_else(|error| panic!("status arguments must be readable: {error}")),
        "-T\n-o\nBatchMode=yes\n-o\nClearAllForwardings=yes\n-o\nForwardAgent=no\n-o\nForwardX11=no\n-o\nStdinNull=no\n-o\nForkAfterAuthentication=no\n--\nfictional-alias\nakp-v1 status\n"
    );
    let sent: StatusRequest = serde_json::from_slice(
        &fs::read(request_file)
            .unwrap_or_else(|error| panic!("status request must be readable: {error}")),
    )
    .unwrap_or_else(|error| panic!("status request must decode: {error}"));
    assert_eq!(sent.request_id, request_id);

    let oversized_program = root.path().join("fictional-oversized-status-ssh");
    write_program(
        &oversized_program,
        "#!/bin/sh\ncat > /dev/null\nhead -c 4097 /dev/zero\n",
    );
    assert!(matches!(
        status_with_program(
            oversized_program.as_os_str(),
            OsStr::new("fictional-alias"),
            &StatusRequest::new(request_id),
            Duration::from_secs(5),
            Vec::new(),
            Vec::new(),
        ),
        Err(ClientCommandError::ControlResponseTooLarge { maximum: 4096 })
    ));
}

#[test]
fn bounds_gateway_responses_before_decoding() {
    let oversized = vec![b'x'; MAXIMUM_RESPONSE_BYTES as usize + 1];
    assert!(matches!(
        read_bounded_response(&mut oversized.as_slice()),
        Err(ClientCommandError::ResponseTooLarge)
    ));
    assert!(matches!(
        read_bounded_diagnostic(&mut oversized.as_slice()),
        Err(ClientCommandError::DiagnosticTooLarge)
    ));
}

#[test]
fn prepared_package_uses_an_immutable_payload_snapshot() {
    let root = TestDirectory::create();
    let package = write_package(root.path());
    let prepared = PreparedPackage::open(&package)
        .unwrap_or_else(|error| panic!("package snapshot must succeed: {error}"));
    fs::write(
        package.join("payload/run/index.md"),
        "x".repeat(MARKDOWN.len()),
    )
    .unwrap_or_else(|error| panic!("payload replacement must be written: {error}"));

    let mut bytes = Vec::new();
    prepared
        .write_archive(&mut bytes)
        .unwrap_or_else(|error| panic!("snapshot archive must be written: {error}"));
    let mut archive = Archive::new(bytes.as_slice());
    for entry in archive
        .entries()
        .unwrap_or_else(|error| panic!("snapshot archive must decode: {error}"))
    {
        let mut entry = entry.unwrap_or_else(|error| panic!("snapshot entry must decode: {error}"));
        if entry
            .path()
            .unwrap_or_else(|error| panic!("snapshot path must decode: {error}"))
            == Path::new("payload/run/index.md")
        {
            let mut body = String::new();
            entry
                .read_to_string(&mut body)
                .unwrap_or_else(|error| panic!("snapshot payload must be readable: {error}"));
            assert_eq!(body, MARKDOWN);
            return;
        }
    }
    panic!("snapshot payload entry must exist");
}

#[test]
fn rejects_a_same_length_change_before_network_output() {
    let root = TestDirectory::create();
    let package = write_package(root.path());
    let validated = validate_package(&package, &PackagePolicy::default())
        .unwrap_or_else(|error| panic!("package fixture must validate: {error}"));
    fs::write(
        package.join("payload/run/index.md"),
        "x".repeat(MARKDOWN.len()),
    )
    .unwrap_or_else(|error| panic!("changed payload must be written: {error}"));
    let pinned = PinnedDirectory::open(&package)
        .unwrap_or_else(|error| panic!("package root must be pinned: {error}"));

    assert!(matches!(
        open_payload(&pinned, &validated.payload()[0]),
        Err(ClientCommandError::PackageChanged { .. })
    ));
}

#[cfg(target_os = "linux")]
#[test]
fn rejects_a_payload_path_replaced_by_a_symbolic_link() {
    use std::os::unix::fs::symlink;

    let root = TestDirectory::create();
    let package = write_package(root.path());
    let validated = validate_package(&package, &PackagePolicy::default())
        .unwrap_or_else(|error| panic!("package fixture must validate: {error}"));
    let payload = package.join("payload/run/index.md");
    let replacement = root.path().join("fictional-replacement.md");
    fs::write(&replacement, MARKDOWN)
        .unwrap_or_else(|error| panic!("replacement payload must be written: {error}"));
    fs::remove_file(&payload)
        .unwrap_or_else(|error| panic!("original payload must be removed: {error}"));
    symlink(&replacement, &payload)
        .unwrap_or_else(|error| panic!("payload symlink must be created: {error}"));
    let pinned = PinnedDirectory::open(&package)
        .unwrap_or_else(|error| panic!("package root must be pinned: {error}"));

    assert!(matches!(
        open_payload(&pinned, &validated.payload()[0]),
        Err(ClientCommandError::OpenPayload { .. })
    ));
}

#[cfg(target_os = "linux")]
#[test]
fn rejects_a_payload_parent_replaced_by_a_symbolic_link() {
    use std::os::unix::fs::symlink;

    let root = TestDirectory::create();
    let package = write_package(root.path());
    let validated = validate_package(&package, &PackagePolicy::default())
        .unwrap_or_else(|error| panic!("package fixture must validate: {error}"));
    let outside = root.path().join("fictional-outside");
    fs::create_dir(&outside)
        .unwrap_or_else(|error| panic!("outside fixture must be created: {error}"));
    fs::write(outside.join("index.md"), MARKDOWN)
        .unwrap_or_else(|error| panic!("outside payload must be written: {error}"));
    fs::remove_dir_all(package.join("payload/run"))
        .unwrap_or_else(|error| panic!("original payload parent must be removed: {error}"));
    symlink(&outside, package.join("payload/run"))
        .unwrap_or_else(|error| panic!("payload parent symlink must be created: {error}"));
    let pinned = PinnedDirectory::open(&package)
        .unwrap_or_else(|error| panic!("package root must be pinned: {error}"));

    assert!(matches!(
        open_payload(&pinned, &validated.payload()[0]),
        Err(ClientCommandError::OpenPayload { .. })
    ));
}

#[cfg(unix)]
#[test]
fn decodes_a_bounded_gateway_rejection() {
    let root = TestDirectory::create();
    let package = write_package(root.path());
    let program = root.path().join("fictional-ssh");
    write_program(
        &program,
        "#!/bin/sh\ncat > /dev/null\nprintf '%s\\n' '{\"protocol_version\":1,\"error_code\":\"REVISION_CONFLICT\"}' >&2\nexit 2\n",
    );

    let error = match submit_with_program(
        program.as_os_str(),
        OsStr::new("fictional-alias"),
        &package,
        Duration::from_secs(5),
        Vec::new(),
        Vec::new(),
    ) {
        Ok(()) => panic!("Gateway rejection must fail"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        ClientCommandError::GatewayRejected(ProtocolErrorResponse { .. })
    ));
    let mut diagnostic = Vec::new();
    error
        .write_diagnostic(&mut diagnostic)
        .unwrap_or_else(|write_error| panic!("Gateway error must encode: {write_error}"));
    assert_eq!(
        diagnostic,
        b"{\"protocol_version\":1,\"error_code\":\"REVISION_CONFLICT\"}\n"
    );
}

#[cfg(unix)]
#[test]
fn extracts_a_gateway_rejection_after_ssh_warnings() {
    let root = TestDirectory::create();
    let package = write_package(root.path());
    let program = root.path().join("fictional-ssh");
    write_program(
        &program,
        "#!/bin/sh\ncat > /dev/null\nprintf '%s\\n' 'Warning: fictional proxy' >&2\nprintf '%s\\n' '{\"protocol_version\":1,\"error_code\":\"TEMPORARY_FAILURE\"}' >&2\nexit 2\n",
    );

    let error = match submit_with_program(
        program.as_os_str(),
        OsStr::new("fictional-alias"),
        &package,
        Duration::from_secs(5),
        Vec::new(),
        Vec::new(),
    ) {
        Ok(()) => panic!("Gateway rejection after an SSH warning must fail"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        ClientCommandError::GatewayRejected(ProtocolErrorResponse { .. })
    ));
    let mut diagnostic = Vec::new();
    error
        .write_diagnostic(&mut diagnostic)
        .unwrap_or_else(|write_error| panic!("Gateway error must encode: {write_error}"));
    assert_eq!(
        diagnostic,
        b"{\"protocol_version\":1,\"error_code\":\"TEMPORARY_FAILURE\"}\n"
    );
}

#[cfg(unix)]
#[test]
fn terminates_a_stalled_ssh_process_at_the_deadline() {
    let root = TestDirectory::create();
    let package = write_package(root.path());
    let program = root.path().join("fictional-ssh");
    write_program(&program, "#!/bin/sh\ncat > /dev/null\nexec sleep 5\n");

    assert!(matches!(
        submit_with_program(
            program.as_os_str(),
            OsStr::new("fictional-alias"),
            &package,
            Duration::from_millis(50),
            Vec::new(),
            Vec::new(),
        ),
        Err(ClientCommandError::SshTimedOut { .. })
    ));
}

#[cfg(unix)]
#[test]
fn terminates_descendants_that_keep_ssh_streams_open() {
    let root = TestDirectory::create();
    let package = write_package(root.path());
    let program = root.path().join("fictional-ssh");
    write_program(&program, "#!/bin/sh\ncat > /dev/null\nsleep 5 &\nexit 0\n");
    let started = Instant::now();

    assert!(matches!(
        submit_with_program(
            program.as_os_str(),
            OsStr::new("fictional-alias"),
            &package,
            Duration::from_millis(50),
            Vec::new(),
            Vec::new(),
        ),
        Err(ClientCommandError::SshTimedOut { .. })
    ));
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[cfg(unix)]
#[test]
fn cancels_ssh_immediately_when_stdout_exceeds_its_bound() {
    let root = TestDirectory::create();
    let package = write_package(root.path());
    let program = root.path().join("fictional-ssh");
    write_program(
        &program,
        "#!/bin/sh\ncat > /dev/null\nhead -c 65537 /dev/zero\nexec sleep 5\n",
    );
    let started = Instant::now();

    assert!(matches!(
        submit_with_program(
            program.as_os_str(),
            OsStr::new("fictional-alias"),
            &package,
            Duration::from_secs(5),
            Vec::new(),
            Vec::new(),
        ),
        Err(ClientCommandError::ResponseTooLarge)
    ));
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[cfg(unix)]
#[test]
fn cancels_ssh_immediately_when_stderr_exceeds_its_bound() {
    let root = TestDirectory::create();
    let package = write_package(root.path());
    let program = root.path().join("fictional-ssh");
    write_program(
        &program,
        "#!/bin/sh\ncat > /dev/null\nhead -c 65537 /dev/zero >&2\nexec sleep 5\n",
    );
    let started = Instant::now();

    assert!(matches!(
        submit_with_program(
            program.as_os_str(),
            OsStr::new("fictional-alias"),
            &package,
            Duration::from_secs(5),
            Vec::new(),
            Vec::new(),
        ),
        Err(ClientCommandError::DiagnosticTooLarge)
    ));
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[cfg(unix)]
#[test]
fn identifies_a_future_success_protocol_before_decoding_its_body() {
    let root = TestDirectory::create();
    let package = write_package(root.path());
    let program = root.path().join("fictional-ssh");
    write_program(
        &program,
        "#!/bin/sh\ncat > /dev/null\nprintf '%s\\n' '{\"protocol_version\":2,\"status\":\"queued\",\"ticket\":\"fictional\"}'\n",
    );

    assert!(matches!(
        submit_with_program(
            program.as_os_str(),
            OsStr::new("fictional-alias"),
            &package,
            Duration::from_secs(5),
            Vec::new(),
            Vec::new(),
        ),
        Err(ClientCommandError::UnsupportedProtocolVersion { actual: 2 })
    ));
}

#[cfg(unix)]
#[test]
fn identifies_a_future_error_protocol_before_decoding_its_body() {
    let root = TestDirectory::create();
    let package = write_package(root.path());
    let program = root.path().join("fictional-ssh");
    write_program(
        &program,
        "#!/bin/sh\ncat > /dev/null\nprintf '%s\\n' '{\"protocol_version\":2,\"error_code\":\"RATE_LIMITED\",\"retry_after\":1}' >&2\nexit 2\n",
    );

    assert!(matches!(
        submit_with_program(
            program.as_os_str(),
            OsStr::new("fictional-alias"),
            &package,
            Duration::from_secs(5),
            Vec::new(),
            Vec::new(),
        ),
        Err(ClientCommandError::UnsupportedProtocolVersion { actual: 2 })
    ));
}

#[test]
fn rejects_duplicate_protocol_version_fields() {
    assert!(decode_protocol_version(b"{\"protocol_version\":1,\"protocol_version\":2}").is_err());
}

#[test]
fn rejects_success_responses_for_a_different_package() {
    let root = TestDirectory::create();
    let package = write_package(root.path());
    let prepared = PreparedPackage::open(&package)
        .unwrap_or_else(|error| panic!("package expectation must be prepared: {error}"));
    for response in [
        "{\"protocol_version\":1,\"status\":\"accepted\",\"request_id\":\"01K00000000000000000000009\",\"digest\":\"sha256:0000000000000000000000000000000000000000000000000000000000000000\"}",
        "{\"protocol_version\":1,\"status\":\"existing\",\"request_id\":\"01K00000000000000000000009\",\"digest\":\"sha256:0000000000000000000000000000000000000000000000000000000000000000\",\"state\":\"pending\"}",
    ] {
        let response: SubmitResponse = serde_json::from_str(response)
            .unwrap_or_else(|error| panic!("mismatched response fixture must decode: {error}"));
        assert!(matches!(
            prepared.expectation().verify(&response),
            Err(ClientCommandError::ResponseMismatch)
        ));
    }
}

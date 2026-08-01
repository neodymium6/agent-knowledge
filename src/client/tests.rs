use std::ffi::OsStr;
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use tar::Archive;

use super::{
    ClientCommandError, MAXIMUM_RESPONSE_BYTES, read_bounded_response, submit_with_program,
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
const RESPONSE: &str = "{\"protocol_version\":1,\"status\":\"accepted\",\"request_id\":\"01K00000000000000000000000\",\"digest\":\"sha256:0000000000000000000000000000000000000000000000000000000000000000\"}";

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

#[cfg(unix)]
#[test]
fn invokes_system_ssh_without_a_shell_and_streams_a_valid_archive() {
    use std::os::unix::fs::PermissionsExt;

    let root = TestDirectory::create();
    let package = write_package(root.path());
    let arguments = root.path().join("arguments");
    let archive = root.path().join("archive.tar");
    let program = root.path().join("fictional-ssh");
    let script = format!(
        "#!/bin/sh\nset -eu\nprintf '%s\\n' \"$@\" > '{}'\ncat > '{}'\nprintf '%s\\n' '{}'\n",
        arguments.display(),
        archive.display(),
        RESPONSE
    );
    fs::write(&program, script)
        .unwrap_or_else(|error| panic!("fake ssh program must be written: {error}"));
    fs::set_permissions(&program, fs::Permissions::from_mode(0o700))
        .unwrap_or_else(|error| panic!("fake ssh program must be executable: {error}"));

    let mut output = Vec::new();
    submit_with_program(
        program.as_os_str(),
        OsStr::new("-fictional-alias"),
        &package,
        &mut output,
    )
    .unwrap_or_else(|error| panic!("client submission must succeed: {error}"));

    let invoked = fs::read_to_string(arguments)
        .unwrap_or_else(|error| panic!("fake ssh arguments must be readable: {error}"));
    assert_eq!(
        invoked,
        "-T\n-o\nBatchMode=yes\n-o\nClearAllForwardings=yes\n--\n-fictional-alias\nakp-v1 submit\n"
    );
    assert_eq!(String::from_utf8_lossy(&output), format!("{RESPONSE}\n"));

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

#[test]
fn bounds_gateway_responses_before_decoding() {
    let oversized = vec![b'x'; MAXIMUM_RESPONSE_BYTES as usize + 1];
    assert!(matches!(
        read_bounded_response(&mut oversized.as_slice()),
        Err(ClientCommandError::ResponseTooLarge)
    ));
}

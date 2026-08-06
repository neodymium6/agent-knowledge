#![cfg(unix)]

use std::fs;
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use agent_knowledge_queue::{PackagePolicy, validate_package};

const DOCUMENT_ID: &str = "01K00000000000000000000004";
const REQUEST_ID: &str = "01K00000000000000000000005";
const REVISION: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[cfg(unix)]
#[test]
fn archives_a_document_through_the_stdio_mcp_server() {
    let fixture = tempfile::tempdir()
        .unwrap_or_else(|error| panic!("test directory must be created: {error}"));
    let submitted_archive = fixture.path().join("submitted.tar");
    let ssh = fixture.path().join("ssh");
    write_fake_ssh(&ssh, &submitted_archive, fixture.path());
    let search_path = std::env::join_paths(
        std::iter::once(fixture.path().to_path_buf()).chain(
            std::env::var_os("PATH")
                .into_iter()
                .flat_map(|value| std::env::split_paths(&value).collect::<Vec<_>>()),
        ),
    )
    .unwrap_or_else(|error| panic!("fictional PATH must be constructed: {error}"));

    let mut child = Command::new(env!("CARGO_BIN_EXE_agent-knowledge-client"))
        .args(["mcp", "--destination", "fictional-knowledge"])
        .env("PATH", search_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|error| panic!("MCP client process must start: {error}"));

    let mut input = child
        .stdin
        .take()
        .unwrap_or_else(|| panic!("MCP client stdin must be available"));
    writeln!(
        input,
        r#"{{"jsonrpc":"2.0","id":1,"method":"initialize","params":{{"protocolVersion":"2025-06-18","capabilities":{{}},"clientInfo":{{"name":"fictional-client","version":"0.0.0"}}}}}}"#
    )
    .unwrap_or_else(|error| panic!("initialize request must be written: {error}"));
    writeln!(
        input,
        r#"{{"jsonrpc":"2.0","method":"notifications/initialized"}}"#
    )
    .unwrap_or_else(|error| panic!("initialized notification must be written: {error}"));
    writeln!(
        input,
        r#"{{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{{}}}}"#
    )
    .unwrap_or_else(|error| panic!("tools/list request must be written: {error}"));
    writeln!(
        input,
        "{}",
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "knowledge_archive_document",
                "arguments": {
                    "document_id": DOCUMENT_ID,
                    "expected_revision": REVISION,
                    "request_id": REQUEST_ID,
                    "created_at": "2026-08-06T10:00:00Z"
                }
            }
        })
    )
    .unwrap_or_else(|error| panic!("archive tool request must be written: {error}"));
    drop(input);

    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = child
            .try_wait()
            .unwrap_or_else(|error| panic!("MCP client status must be readable: {error}"))
        {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            panic!("MCP client did not stop after standard input closed");
        }
        thread::sleep(Duration::from_millis(10));
    };

    let mut output = String::new();
    child
        .stdout
        .take()
        .unwrap_or_else(|| panic!("MCP client stdout must be available"))
        .read_to_string(&mut output)
        .unwrap_or_else(|error| panic!("MCP response must be readable: {error}"));
    let mut diagnostic = String::new();
    child
        .stderr
        .take()
        .unwrap_or_else(|| panic!("MCP client stderr must be available"))
        .read_to_string(&mut diagnostic)
        .unwrap_or_else(|error| panic!("MCP diagnostics must be readable: {error}"));

    assert!(status.success(), "MCP client failed: {diagnostic}");
    let responses = output
        .lines()
        .map(|line| {
            serde_json::from_str::<serde_json::Value>(line).unwrap_or_else(|error| {
                panic!("MCP response must be JSON: {error}; response: {line}")
            })
        })
        .collect::<Vec<_>>();
    let response = responses
        .iter()
        .find(|response| response["id"] == 1)
        .unwrap_or_else(|| panic!("initialize response must be present: {output}"));
    assert_eq!(
        response["result"]["serverInfo"]["name"],
        "agent-knowledge-client"
    );
    assert!(response["result"]["capabilities"]["tools"].is_object());
    let tools = responses
        .iter()
        .find(|response| response["id"] == 2)
        .and_then(|response| response["result"]["tools"].as_array())
        .unwrap_or_else(|| panic!("tools/list response must be present: {output}"));
    assert!(tools.iter().any(|tool| {
        tool["name"] == "knowledge_archive_document"
            && tool["inputSchema"]["properties"]
                .get("package_root")
                .is_none()
    }));
    let archive = responses
        .iter()
        .find(|response| response["id"] == 3)
        .unwrap_or_else(|| panic!("archive response must be present: {output}"));
    assert_eq!(
        archive["result"]["structuredContent"]["request_id"],
        REQUEST_ID
    );
    assert!(submitted_archive.is_file());
}

#[cfg(unix)]
fn write_fake_ssh(
    ssh: &std::path::Path,
    submitted_archive: &std::path::Path,
    root: &std::path::Path,
) {
    let expected_package = root.join("expected-package");
    fs::create_dir_all(expected_package.join("payload"))
        .unwrap_or_else(|error| panic!("expected payload directory must be created: {error}"));
    fs::write(
        expected_package.join("request.json"),
        serde_json::to_vec(&serde_json::json!({
            "protocol_version": 1,
            "request_id": REQUEST_ID,
            "title": format!("Archive document {DOCUMENT_ID}"),
            "project": "fictional-solver",
            "document_type": "experiment",
            "created_at": "2026-08-06T10:00:00Z",
            "operations": [{
                "type": "archive_document",
                "document_id": DOCUMENT_ID,
                "expected_revision": REVISION
            }]
        }))
        .unwrap_or_else(|error| panic!("expected request must encode: {error}")),
    )
    .unwrap_or_else(|error| panic!("expected request must be written: {error}"));
    let package = validate_package(&expected_package, &PackagePolicy::default())
        .unwrap_or_else(|error| panic!("expected request package must validate: {error}"));
    let submit_response = serde_json::json!({
        "protocol_version": 1,
        "status": "accepted",
        "request_id": REQUEST_ID,
        "digest": package.digest().to_string()
    })
    .to_string();
    let get_response = serde_json::json!({
        "protocol_version": 1,
        "commit": "0123456789abcdef0123456789abcdef01234567",
        "document": {
            "summary": {
                "path": "projects/fictional-solver/experiments/2026/08/fictional-result.md",
                "document_type": "experiment",
                "project": "fictional-solver",
                "archived": false,
                "revision": REVISION,
                "metadata": {
                    "schema_version": 1,
                    "document_id": DOCUMENT_ID,
                    "title": "Fictional result",
                    "created": "2026-08-05T10:00:00Z",
                    "request_id": "01K00000000000000000000006",
                    "status": "active"
                }
            },
            "markdown": "---\n---\n\nFictional result."
        }
    })
    .to_string();
    let script = format!(
        "#!/bin/sh\nset -eu\ncase \"$*\" in\n  *\"akp-v1 get\") cat >/dev/null; printf '%s\\n' '{get_response}' ;;\n  *\"akp-v1 submit\") cat >'{submitted}'; printf '%s\\n' '{submit_response}' ;;\n  *) exit 64 ;;\nesac\n",
        submitted = submitted_archive.display(),
    );
    fs::write(ssh, script)
        .unwrap_or_else(|error| panic!("fake SSH program must be written: {error}"));
    fs::set_permissions(ssh, fs::Permissions::from_mode(0o700))
        .unwrap_or_else(|error| panic!("fake SSH program must be executable: {error}"));
}

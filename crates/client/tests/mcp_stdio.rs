use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use agent_knowledge_queue::{PackagePolicy, validate_package};

#[cfg(unix)]
use std::{fs, io};

#[cfg(unix)]
const REQUEST_JSON: &str = r#"{
  "protocol_version": 1,
  "request_id": "01K00000000000000000000000",
  "title": "Record a fictional MCP test",
  "project": "fictional-project",
  "document_type": "experiment",
  "created_at": "2026-07-31T03:50:00Z",
  "operations": [{
    "type": "create_document",
    "document_id": "01K00000000000000000000001",
    "content": "run/index.md"
  }]
}"#;

#[cfg(unix)]
const MARKDOWN: &str = "---\n\
schema_version: 1\n\
document_id: 01K00000000000000000000001\n\
title: Fictional MCP test\n\
created: 2026-07-31T03:50:00Z\n\
request_id: 01K00000000000000000000000\n\
tags: []\n\
status: active\n\
---\n";

#[test]
fn initializes_the_stdio_mcp_server() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_agent-knowledge-client"))
        .args(["mcp", "--destination", "fictional-knowledge"])
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
    let response: serde_json::Value = serde_json::from_str(output.trim())
        .unwrap_or_else(|error| panic!("MCP response must be JSON: {error}; response: {output}"));
    assert_eq!(response["id"], 1);
    assert_eq!(
        response["result"]["serverInfo"]["name"],
        "agent-knowledge-client"
    );
    assert!(response["result"]["capabilities"]["tools"].is_object());
}

#[cfg(unix)]
#[test]
fn cancels_an_inflight_ssh_operation() {
    use std::os::unix::fs::PermissionsExt;

    let root = TestDirectory::create();
    let marker = root.path().join("ssh-started");
    let process_id = root.path().join("ssh-process-id");
    let ssh = root.path().join("ssh");
    fs::write(
        &ssh,
        format!(
            "#!/bin/sh\nset -eu\nprintf '%s' \"$$\" > '{}'\nprintf started > '{}'\ncat > /dev/null\nexec sleep 30\n",
            process_id.display(),
            marker.display(),
        ),
    )
    .unwrap_or_else(|error| panic!("fake ssh must be written: {error}"));
    fs::set_permissions(&ssh, fs::Permissions::from_mode(0o700))
        .unwrap_or_else(|error| panic!("fake ssh must be executable: {error}"));
    let child = start_mcp_process(root.path());
    let mut child = ProcessGuard::new(child, process_id.clone());
    let mut input = child
        .stdin
        .take()
        .unwrap_or_else(|| panic!("MCP client stdin must be available"));
    let mut output = BufReader::new(
        child
            .stdout
            .take()
            .unwrap_or_else(|| panic!("MCP client stdout must be available")),
    );

    initialize(&mut input, &mut output);
    writeln!(
        input,
        r#"{{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{{"name":"knowledge_list","arguments":{{}}}}}}"#
    )
    .unwrap_or_else(|error| panic!("tool request must be written: {error}"));
    input
        .flush()
        .unwrap_or_else(|error| panic!("tool request must flush: {error}"));
    wait_for_file(&marker, Duration::from_secs(2));

    writeln!(
        input,
        r#"{{"jsonrpc":"2.0","method":"notifications/cancelled","params":{{"requestId":2,"reason":"fictional cancellation test"}}}}"#
    )
    .unwrap_or_else(|error| panic!("cancellation notification must be written: {error}"));
    input
        .flush()
        .unwrap_or_else(|error| panic!("cancellation notification must flush: {error}"));
    thread::sleep(Duration::from_millis(100));
    drop(input);

    let status = wait_for_process(&mut child, Duration::from_secs(2))
        .unwrap_or_else(|| panic!("MCP client did not stop after request cancellation"));
    let mut diagnostic = String::new();
    child
        .stderr
        .take()
        .unwrap_or_else(|| panic!("MCP client stderr must be available"))
        .read_to_string(&mut diagnostic)
        .unwrap_or_else(|error| panic!("MCP diagnostics must be readable: {error}"));
    assert!(status.success(), "MCP client failed: {diagnostic}");
}

#[cfg(unix)]
#[test]
fn closes_an_inflight_ssh_operation_on_stdio_eof() {
    use std::os::unix::fs::PermissionsExt;

    let root = TestDirectory::create();
    let marker = root.path().join("ssh-started");
    let process_id = root.path().join("ssh-process-id");
    let ssh = root.path().join("ssh");
    fs::write(
        &ssh,
        format!(
            "#!/bin/sh\nset -eu\nprintf '%s' \"$$\" > '{}'\nprintf started > '{}'\ncat > /dev/null\nexec sleep 30\n",
            process_id.display(),
            marker.display(),
        ),
    )
    .unwrap_or_else(|error| panic!("fake ssh must be written: {error}"));
    fs::set_permissions(&ssh, fs::Permissions::from_mode(0o700))
        .unwrap_or_else(|error| panic!("fake ssh must be executable: {error}"));

    let child = start_mcp_process(root.path());
    let mut child = ProcessGuard::new(child, process_id);
    let mut input = child
        .stdin
        .take()
        .unwrap_or_else(|| panic!("MCP client stdin must be available"));
    let mut output = BufReader::new(
        child
            .stdout
            .take()
            .unwrap_or_else(|| panic!("MCP client stdout must be available")),
    );
    initialize(&mut input, &mut output);
    writeln!(
        input,
        r#"{{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{{"name":"knowledge_list","arguments":{{}}}}}}"#
    )
    .unwrap_or_else(|error| panic!("tool request must be written: {error}"));
    input
        .flush()
        .unwrap_or_else(|error| panic!("tool request must flush: {error}"));
    wait_for_file(&marker, Duration::from_secs(2));

    let disconnected_at = Instant::now();
    drop(input);
    let status = wait_for_process(&mut child, Duration::from_secs(2))
        .unwrap_or_else(|| panic!("MCP client did not stop after standard input closed"));
    assert!(
        disconnected_at.elapsed() < Duration::from_secs(2),
        "MCP shutdown exceeded its bound"
    );
    assert!(status.success(), "MCP client failed");
}

#[cfg(unix)]
#[test]
fn bounds_parallel_ssh_operations() {
    use std::os::unix::fs::PermissionsExt;

    let root = TestDirectory::create();
    let process_ids = root.path().join("ssh-process-ids");
    fs::create_dir(&process_ids)
        .unwrap_or_else(|error| panic!("process ID directory must be created: {error}"));
    let ssh = root.path().join("ssh");
    fs::write(
        &ssh,
        format!(
            "#!/bin/sh\nset -eu\nprintf '%s' \"$$\" > '{}/'\"$$\"\ncat > /dev/null\nexec sleep 30\n",
            process_ids.display(),
        ),
    )
    .unwrap_or_else(|error| panic!("fake ssh must be written: {error}"));
    fs::set_permissions(&ssh, fs::Permissions::from_mode(0o700))
        .unwrap_or_else(|error| panic!("fake ssh must be executable: {error}"));

    let child = start_mcp_process(root.path());
    let mut child = ProcessGuard::new(child, process_ids.clone());
    let mut input = child
        .stdin
        .take()
        .unwrap_or_else(|| panic!("MCP client stdin must be available"));
    let mut output = BufReader::new(
        child
            .stdout
            .take()
            .unwrap_or_else(|| panic!("MCP client stdout must be available")),
    );
    initialize(&mut input, &mut output);

    for request_id in 2..=9 {
        writeln!(
            input,
            r#"{{"jsonrpc":"2.0","id":{request_id},"method":"tools/call","params":{{"name":"knowledge_list","arguments":{{}}}}}}"#
        )
        .unwrap_or_else(|error| panic!("tool request must be written: {error}"));
    }
    input
        .flush()
        .unwrap_or_else(|error| panic!("tool requests must flush: {error}"));
    wait_for_file_count(&process_ids, 4, Duration::from_secs(2));
    thread::sleep(Duration::from_millis(200));
    assert_eq!(directory_entry_count(&process_ids), 4);

    drop(input);
    let status = wait_for_process(&mut child, Duration::from_secs(2))
        .unwrap_or_else(|| panic!("MCP client did not stop after standard input closed"));
    assert!(status.success(), "MCP client failed");
}

#[test]
fn rejects_an_oversized_input_line() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_agent-knowledge-client"))
        .args(["mcp", "--destination", "fictional-knowledge"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|error| panic!("MCP client process must start: {error}"));
    let mut input = child
        .stdin
        .take()
        .unwrap_or_else(|| panic!("MCP client stdin must be available"));
    let mut output = BufReader::new(
        child
            .stdout
            .take()
            .unwrap_or_else(|| panic!("MCP client stdout must be available")),
    );
    initialize(&mut input, &mut output);
    let oversized = vec![b'x'; 1024 * 1024 + 1];
    let _ = input.write_all(&oversized);
    drop(input);

    let status = wait_for_process(&mut child, Duration::from_secs(2))
        .unwrap_or_else(|| panic!("MCP client did not reject an oversized input line"));
    assert!(
        status.success(),
        "MCP client failed while closing the connection"
    );
}

#[cfg(unix)]
#[test]
fn reports_sanitized_ssh_diagnostics() {
    use std::os::unix::fs::PermissionsExt;

    let root = TestDirectory::create();
    let ssh = root.path().join("ssh");
    fs::write(
        &ssh,
        "#!/bin/sh\nset -eu\ncat > /dev/null\nprintf 'fictional host key rejected\\033[31m\\n' >&2\nexit 255\n",
    )
    .unwrap_or_else(|error| panic!("fake ssh must be written: {error}"));
    fs::set_permissions(&ssh, fs::Permissions::from_mode(0o700))
        .unwrap_or_else(|error| panic!("fake ssh must be executable: {error}"));

    let mut child = start_mcp_process(root.path());
    let mut input = child
        .stdin
        .take()
        .unwrap_or_else(|| panic!("MCP client stdin must be available"));
    let mut output = BufReader::new(
        child
            .stdout
            .take()
            .unwrap_or_else(|| panic!("MCP client stdout must be available")),
    );
    initialize(&mut input, &mut output);
    writeln!(
        input,
        r#"{{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{{"name":"knowledge_list","arguments":{{}}}}}}"#
    )
    .unwrap_or_else(|error| panic!("tool request must be written: {error}"));
    input
        .flush()
        .unwrap_or_else(|error| panic!("tool request must flush: {error}"));
    let mut response = String::new();
    output
        .read_line(&mut response)
        .unwrap_or_else(|error| panic!("tool response must be readable: {error}"));
    assert!(response.contains("fictional host key rejected[31m"));
    assert!(!response.contains("\\u001b"));
    assert!(response.contains("ssh exited unsuccessfully"));

    drop(input);
    let status = wait_for_process(&mut child, Duration::from_secs(2))
        .unwrap_or_else(|| panic!("MCP client did not stop after standard input closed"));
    assert!(status.success(), "MCP client failed");
}

#[cfg(unix)]
#[test]
fn submits_through_the_isolated_helper() {
    use std::os::unix::fs::PermissionsExt;

    let root = TestDirectory::create();
    let package = root.path().join("package");
    fs::create_dir_all(package.join("payload/run"))
        .unwrap_or_else(|error| panic!("package directories must be created: {error}"));
    fs::write(package.join("request.json"), REQUEST_JSON)
        .unwrap_or_else(|error| panic!("request fixture must be written: {error}"));
    fs::write(package.join("payload/run/index.md"), MARKDOWN)
        .unwrap_or_else(|error| panic!("Markdown fixture must be written: {error}"));
    let digest = validate_package(&package, &PackagePolicy::default())
        .unwrap_or_else(|error| panic!("package fixture must validate: {error}"))
        .digest();
    let ssh = root.path().join("ssh");
    fs::write(
        &ssh,
        format!(
            "#!/bin/sh\nset -eu\ncat > /dev/null\nprintf '%s\\n' '{{\"protocol_version\":1,\"status\":\"accepted\",\"request_id\":\"01K00000000000000000000000\",\"digest\":\"{digest}\"}}'\n",
        ),
    )
    .unwrap_or_else(|error| panic!("fake ssh must be written: {error}"));
    fs::set_permissions(&ssh, fs::Permissions::from_mode(0o700))
        .unwrap_or_else(|error| panic!("fake ssh must be executable: {error}"));

    let mut child = start_mcp_process(root.path());
    let mut input = child
        .stdin
        .take()
        .unwrap_or_else(|| panic!("MCP client stdin must be available"));
    let mut output = BufReader::new(
        child
            .stdout
            .take()
            .unwrap_or_else(|| panic!("MCP client stdout must be available")),
    );
    initialize(&mut input, &mut output);
    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "knowledge_submit_package",
            "arguments": { "package_root": package }
        }
    });
    writeln!(input, "{request}")
        .unwrap_or_else(|error| panic!("submit request must be written: {error}"));
    input
        .flush()
        .unwrap_or_else(|error| panic!("submit request must flush: {error}"));
    let mut response = String::new();
    output
        .read_line(&mut response)
        .unwrap_or_else(|error| panic!("submit response must be readable: {error}"));
    let response: serde_json::Value = serde_json::from_str(response.trim())
        .unwrap_or_else(|error| panic!("submit response must be JSON: {error}"));
    assert_eq!(
        response["result"]["structuredContent"]["status"],
        "accepted"
    );

    drop(input);
    let status = wait_for_process(&mut child, Duration::from_secs(2))
        .unwrap_or_else(|| panic!("MCP client did not stop after standard input closed"));
    assert!(status.success(), "MCP client failed");
}

#[cfg(unix)]
fn start_mcp_process(root: &std::path::Path) -> std::process::Child {
    let mut paths = vec![root.to_path_buf()];
    if let Some(current) = std::env::var_os("PATH") {
        paths.extend(std::env::split_paths(&current));
    }
    let path = std::env::join_paths(paths)
        .unwrap_or_else(|error| panic!("test PATH must be valid: {error}"));
    Command::new(env!("CARGO_BIN_EXE_agent-knowledge-client"))
        .args([
            "mcp",
            "--destination",
            "fictional-knowledge",
            "--timeout-seconds",
            "30",
        ])
        .env("PATH", &path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|error| panic!("MCP client process must start: {error}"))
}

fn initialize(
    input: &mut std::process::ChildStdin,
    output: &mut BufReader<std::process::ChildStdout>,
) {
    writeln!(
        input,
        r#"{{"jsonrpc":"2.0","id":1,"method":"initialize","params":{{"protocolVersion":"2025-06-18","capabilities":{{}},"clientInfo":{{"name":"fictional-client","version":"0.0.0"}}}}}}"#
    )
    .unwrap_or_else(|error| panic!("initialize request must be written: {error}"));
    input
        .flush()
        .unwrap_or_else(|error| panic!("initialize request must flush: {error}"));
    let mut initialization = String::new();
    output
        .read_line(&mut initialization)
        .unwrap_or_else(|error| panic!("initialize response must be readable: {error}"));
    let response: serde_json::Value = serde_json::from_str(initialization.trim())
        .unwrap_or_else(|error| panic!("initialize response must be JSON: {error}"));
    assert_eq!(response["id"], 1);
    writeln!(
        input,
        r#"{{"jsonrpc":"2.0","method":"notifications/initialized"}}"#
    )
    .unwrap_or_else(|error| panic!("initialized notification must be written: {error}"));
}

#[cfg(unix)]
fn wait_for_file(path: &std::path::Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while !path.is_file() {
        assert!(Instant::now() < deadline, "fake ssh did not start");
        thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(unix)]
fn wait_for_file_count(path: &std::path::Path, expected: usize, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while directory_entry_count(path) < expected {
        assert!(
            Instant::now() < deadline,
            "fake SSH concurrency was not reached"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(unix)]
fn directory_entry_count(path: &std::path::Path) -> usize {
    fs::read_dir(path)
        .unwrap_or_else(|error| panic!("process ID directory must be readable: {error}"))
        .count()
}

fn wait_for_process(
    child: &mut std::process::Child,
    timeout: Duration,
) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child
            .try_wait()
            .unwrap_or_else(|error| panic!("MCP client status must be readable: {error}"))
        {
            return Some(status);
        }
        if Instant::now() >= deadline {
            return None;
        }
        thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(unix)]
fn terminate_fake_ssh(process_id: &std::path::Path) {
    if process_id.is_dir() {
        if let Ok(entries) = fs::read_dir(process_id) {
            for entry in entries.flatten() {
                terminate_fake_ssh(&entry.path());
            }
        }
        return;
    }
    let Ok(process_id) = fs::read_to_string(process_id) else {
        return;
    };
    let Ok(process_id) = process_id.parse::<i32>() else {
        return;
    };
    let _ = nix::sys::signal::killpg(
        nix::unistd::Pid::from_raw(process_id),
        nix::sys::signal::Signal::SIGKILL,
    );
}

#[cfg(unix)]
struct TestDirectory(std::path::PathBuf);

#[cfg(unix)]
static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

#[cfg(unix)]
struct ProcessGuard {
    child: std::process::Child,
    fake_ssh_process_id: std::path::PathBuf,
}

#[cfg(unix)]
impl ProcessGuard {
    fn new(child: std::process::Child, fake_ssh_process_id: std::path::PathBuf) -> Self {
        Self {
            child,
            fake_ssh_process_id,
        }
    }
}

#[cfg(unix)]
impl std::ops::Deref for ProcessGuard {
    type Target = std::process::Child;

    fn deref(&self) -> &Self::Target {
        &self.child
    }
}

#[cfg(unix)]
impl std::ops::DerefMut for ProcessGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.child
    }
}

#[cfg(unix)]
impl Drop for ProcessGuard {
    fn drop(&mut self) {
        terminate_fake_ssh(&self.fake_ssh_process_id);
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[cfg(unix)]
impl TestDirectory {
    fn create() -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "agent-knowledge-mcp-test-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path)
            .unwrap_or_else(|error| panic!("MCP test directory must be created: {error}"));
        Self(path)
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

#[cfg(unix)]
impl Drop for TestDirectory {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.0)
            && error.kind() != io::ErrorKind::NotFound
        {
            panic!("MCP test directory must be removed: {error}");
        }
    }
}

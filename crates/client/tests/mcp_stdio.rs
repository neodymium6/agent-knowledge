use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::{fs, io};

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
    let mut paths = vec![root.path().to_path_buf()];
    if let Some(current) = std::env::var_os("PATH") {
        paths.extend(std::env::split_paths(&current));
    }
    let path = std::env::join_paths(paths)
        .unwrap_or_else(|error| panic!("test PATH must be valid: {error}"));

    let child = Command::new(env!("CARGO_BIN_EXE_agent-knowledge-client"))
        .args([
            "mcp",
            "--destination",
            "fictional-knowledge",
            "--timeout-seconds",
            "30",
        ])
        .env("PATH", path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|error| panic!("MCP client process must start: {error}"));
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
fn wait_for_file(path: &std::path::Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while !path.is_file() {
        assert!(Instant::now() < deadline, "fake ssh did not start");
        thread::sleep(Duration::from_millis(10));
    }
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
        let path =
            std::env::temp_dir().join(format!("agent-knowledge-mcp-test-{}", std::process::id()));
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

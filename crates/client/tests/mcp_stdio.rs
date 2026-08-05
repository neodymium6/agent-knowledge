use std::io::{Read, Write};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

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

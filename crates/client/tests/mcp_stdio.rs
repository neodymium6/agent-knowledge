use std::io::{Read, Write};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn initializes_and_lists_tools_from_the_stdio_mcp_server() {
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
    writeln!(
        input,
        r#"{{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{{}}}}"#
    )
    .unwrap_or_else(|error| panic!("tools/list request must be written: {error}"));
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
}

#![cfg(unix)]

use serde_json::{Value, json};
use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn cli_and_stdio_forward_explicit_modes_and_preserve_json_framing() {
    let root = tempfile::tempdir().unwrap_or_else(|e| panic!("tempdir: {e}"));
    let captured = root.path().join("request.json");
    let response_file = root.path().join("response.json");
    let ssh = root.path().join("ssh");
    fs::write(&ssh, "#!/bin/sh\nset -eu\nfor arg do last=$arg; done\ntest \"$last\" = \"$AK_COMMAND\"\ncat > \"$AK_CAPTURE\"\ncat \"$AK_RESPONSE\"\n").unwrap_or_else(|e| panic!("write: {e}"));
    fs::set_permissions(ssh, fs::Permissions::from_mode(0o700))
        .unwrap_or_else(|e| panic!("chmod: {e}"));
    let path = std::env::join_paths(
        std::iter::once(root.path().to_path_buf()).chain(
            std::env::var_os("PATH")
                .into_iter()
                .flat_map(|p| std::env::split_paths(&p).collect::<Vec<_>>()),
        ),
    )
    .unwrap_or_else(|e| panic!("PATH: {e}"));
    let summary = json!({"path":"projects/fictional-project/references/2026-09-24-01K00000000000000000000001/index.md","document_type":"reference","project":"fictional-project","archived":false,"revision":format!("sha256:{}", "a".repeat(64)),"metadata":{"schema_version":1,"document_id":"01K00000000000000000000001","title":"Fictional reference","created":"2026-09-24T00:00:00Z","updated":null,"request_id":"01K00000000000000000000002","status":"active"}});
    for (action, flags, tool, parameters, query, result) in [
        (
            "projects",
            vec![
                "--query",
                "needle",
                "--search-in",
                "documents",
                "--maximum-results",
                "2",
            ],
            "knowledge_projects",
            json!({"query":"needle","search_in":"documents","maximum_results":2}),
            json!({"operation":"projects","query":"needle","search_in":"documents","maximum_results":2,"description_characters":300,"include_archived":false}),
            json!({"operation":"projects","commit":"a".repeat(40),"projects":[{"project":"fictional-project","index":null,"description":"","description_truncated":false,"document_count":3,"matching_documents":2}],"truncated":false}),
        ),
        (
            "projects",
            vec![],
            "knowledge_projects",
            json!({}),
            json!({"operation":"projects","query":null,"search_in":"project","maximum_results":100,"description_characters":300,"include_archived":false}),
            json!({"operation":"projects","commit":"a".repeat(40),"projects":[],"truncated":false}),
        ),
        (
            "search-excerpts",
            vec![
                "--query",
                "needle",
                "--project",
                "fictional-alpha",
                "--project",
                "fictional-beta",
            ],
            "knowledge_search_excerpts",
            json!({"query":"needle","projects":["fictional-alpha","fictional-beta"]}),
            json!({"operation":"search_excerpts","query":"needle","filter":{"projects":["fictional-alpha","fictional-beta"]},"maximum_results":10,"excerpt_characters":300}),
            json!({"operation":"search_excerpts","commit":"a".repeat(40),"hits":[]}),
        ),
        (
            "search",
            vec![
                "--query",
                "needle",
                "--project",
                "fictional-alpha",
                "--project",
                "fictional-beta",
            ],
            "knowledge_search",
            json!({"query":"needle","projects":["fictional-alpha","fictional-beta"]}),
            json!({"protocol_version":1,"query":"needle","projects":["fictional-alpha","fictional-beta"],"maximum_results":100}),
            json!({"protocol_version":1,"commit":"a".repeat(40),"documents":[]}),
        ),
        (
            "context",
            vec![
                "--project",
                "fictional-project",
                "--selection",
                "balanced",
                "--maximum-documents",
                "3",
                "--recent-documents",
                "1",
            ],
            "knowledge_context",
            json!({"project":"fictional-project","selection":"balanced","maximum_documents":3,"recent_documents":1}),
            json!({"operation":"context_balanced","project":"fictional-project","query":null,"maximum_documents":3,"maximum_characters":20000,"recent_documents":1}),
            json!({"operation":"context","commit":"a".repeat(40),"documents":[],"additional":[],"truncated":false}),
        ),
        (
            "diff",
            vec![
                "--document-id",
                "01K00000000000000000000001",
                "--from-commit",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "--to-commit",
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "--format",
                "hunks",
                "--context-lines",
                "0",
            ],
            "knowledge_diff",
            json!({"document_id":"01K00000000000000000000001","from_commit":"a".repeat(40),"to_commit":"b".repeat(40),"format":"hunks","context_lines":0}),
            json!({"operation":"diff_hunks","document_id":"01K00000000000000000000001","from_commit":"a".repeat(40),"to_commit":"b".repeat(40),"context_lines":0,"maximum_hunks":20,"maximum_diff_bytes":64000}),
            json!({"operation":"diff_hunks","from_commit":"a".repeat(40),"to_commit":"b".repeat(40),"before":summary,"after":summary,"body":{"changed":true,"hunks":[{"from_line":1,"to_line":1,"removed":"old\n","added":"追加\n"}],"truncated":false,"truncation_reason":null}}),
        ),
    ] {
        let inspection = query.get("operation").is_some();
        let expected_request = if inspection {
            json!({"protocol_version":1,"query":query})
        } else {
            query.clone()
        };
        let response = if inspection {
            json!({"protocol_version":1,"result":result})
        } else {
            result
        };
        fs::write(&response_file, response.to_string()).unwrap_or_else(|e| panic!("response: {e}"));
        let command = || {
            let mut command = Command::new(env!("CARGO_BIN_EXE_agent-knowledge-client"));
            command
                .env("PATH", &path)
                .env("AGENT_KNOWLEDGE_UPDATE_CHECK", "off")
                .env(
                    "AK_COMMAND",
                    if inspection {
                        "akp-v1 inspect"
                    } else {
                        "akp-v1 search"
                    },
                )
                .env("AK_CAPTURE", &captured)
                .env("AK_RESPONSE", &response_file);
            command
        };
        let output = command()
            .args([action, "--destination", "fictional-knowledge"])
            .args(flags)
            .output()
            .unwrap_or_else(|e| panic!("CLI: {e}"));
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&output.stdout).unwrap_or_else(|e| panic!("JSON: {e}")),
            response
        );
        let read_request = || {
            serde_json::from_slice::<Value>(
                &fs::read(&captured).unwrap_or_else(|e| panic!("read: {e}")),
            )
            .unwrap_or_else(|e| panic!("JSON: {e}"))
        };
        assert_eq!(read_request(), expected_request);
        let mut child = command()
            .args(["mcp", "--destination", "fictional-knowledge"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| panic!("spawn: {e}"));
        let mut input = child.stdin.take().unwrap_or_else(|| panic!("stdin"));
        for message in [
            json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"fictional-client","version":"0.0.0"}}}),
            json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
            json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":tool,"arguments":parameters}}),
        ] {
            writeln!(input, "{message}").unwrap_or_else(|e| panic!("write: {e}"));
        }
        drop(input);
        let deadline = Instant::now() + Duration::from_secs(10);
        while child
            .try_wait()
            .unwrap_or_else(|e| panic!("wait: {e}"))
            .is_none()
        {
            if Instant::now() >= deadline {
                let _ = child.kill();
                panic!("MCP timeout");
            }
            thread::sleep(Duration::from_millis(10));
        }
        let output = child
            .wait_with_output()
            .unwrap_or_else(|e| panic!("output: {e}"));
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let messages = String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(|line| {
                serde_json::from_str::<Value>(line).unwrap_or_else(|e| panic!("framing: {e}"))
            })
            .collect::<Vec<_>>();
        let call = messages
            .iter()
            .find(|m| m["id"] == 2)
            .unwrap_or_else(|| panic!("tool response"));
        assert_eq!(call["result"]["structuredContent"], response);
        assert_eq!(read_request(), expected_request);
    }
}

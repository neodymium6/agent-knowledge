#![cfg(unix)]
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

const DOCUMENT_ID: &str = "01K00000000000000000000004";

struct Fixture {
    root: tempfile::TempDir,
    path: std::ffi::OsString,
    archive: Vec<u8>,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap_or_else(|e| panic!("fixture: {e}"));
        let markdown = format!(
            "---\nschema_version: 1\ndocument_id: {DOCUMENT_ID}\ntitle: Fictional export\ncreated: 2026-07-31T03:50:00Z\nrequest_id: 01K00000000000000000000000\nstatus: active\n---\nFictional body.\n"
        );
        let mut archive = tar::Builder::new(Vec::new());
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Regular);
        header.set_mode(0o644);
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(0);
        header.set_size(markdown.len() as u64);
        header.set_cksum();
        archive
            .append_data(&mut header, "index.md", markdown.as_bytes())
            .unwrap_or_else(|e| panic!("fixture: {e}"));
        let archive = archive
            .into_inner()
            .unwrap_or_else(|e| panic!("fixture: {e}"));
        fs::write(root.path().join("export.tar"), &archive)
            .unwrap_or_else(|e| panic!("fixture: {e}"));
        let ssh = root.path().join("ssh");
        fs::write(&ssh, r##"#!/bin/sh
set -eu
for arg do last=$arg; done
cat >/dev/null
case "$last" in
  'akp-v1 list') printf '%s\n' '{"protocol_version":1,"commit":"fictional-commit","documents":[]}' ;;
  'akp-v1 export') cat "$FIXTURE_ROOT/export.tar" ;;
  'akp-v1 version') printf '%s\n' '{"protocol_version":1,"gateway_version":"99.0.0","commands":["akp-v1 version"],"inspect_queries":[]}' ;;
  *) exit 1 ;;
esac
"##).unwrap_or_else(|e| panic!("fixture: {e}"));
        fs::set_permissions(&ssh, fs::Permissions::from_mode(0o700))
            .unwrap_or_else(|e| panic!("fixture: {e}"));
        let path = std::env::join_paths(
            std::iter::once(root.path().to_owned()).chain(
                std::env::var_os("PATH")
                    .into_iter()
                    .flat_map(|path| std::env::split_paths(&path).collect::<Vec<_>>()),
            ),
        )
        .unwrap_or_else(|e| panic!("fixture: {e}"));
        Self {
            root,
            path,
            archive,
        }
    }

    fn cache(&self) -> PathBuf {
        self.root.path().join("cache")
    }

    fn seed(&self) {
        fs::create_dir_all(self.cache()).unwrap_or_else(|e| panic!("fixture: {e}"));
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        fs::write(self.cache().join("stable-release-v1.json"), serde_json::json!({
            "schema_version":1,"last_attempt_at":now,"checked_at":now,"last_error":null,"notified_version":null,
            "latest":{"version":"9.8.7","url":"https://github.com/neodymium6/agent-knowledge/releases/tag/v9.8.7"}
        }).to_string()).unwrap_or_else(|e| panic!("fixture: {e}"));
    }

    fn run(&self, args: &[&str], policy: &str, cache: &Path) -> Output {
        Command::new(env!("CARGO_BIN_EXE_agent-knowledge-client"))
            .args(args)
            .env("PATH", &self.path)
            .env("FIXTURE_ROOT", self.root.path())
            .env("AGENT_KNOWLEDGE_UPDATE_CHECK", policy)
            .env("AGENT_KNOWLEDGE_CACHE_DIR", cache)
            .output()
            .unwrap_or_else(|e| panic!("client: {e}"))
    }
}

#[test]
fn automatic_notices_preserve_json_export_bytes_and_exit_status() {
    let fixture = Fixture::new();
    for args in [
        vec!["list", "--destination", "fictional-knowledge"],
        vec![
            "export",
            "--destination",
            "fictional-knowledge",
            "--document-id",
            DOCUMENT_ID,
        ],
    ] {
        fixture.seed();
        let first = fixture.run(&args, "on", &fixture.cache());
        assert!(
            first.status.success(),
            "{}",
            String::from_utf8_lossy(&first.stderr)
        );
        assert!(String::from_utf8_lossy(&first.stderr).contains("9.8.7"));
        if args[0] == "export" {
            assert_eq!(first.stdout, fixture.archive);
        } else {
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&first.stdout)
                    .unwrap_or_else(|e| panic!("JSON: {e}"))["commit"],
                "fictional-commit"
            );
        }
        let second = fixture.run(&args, "on", &fixture.cache());
        assert!(second.status.success() && second.stderr.is_empty());
        assert_eq!(second.stdout, first.stdout);
        let disabled = fixture.run(&args, "off", &fixture.cache());
        assert!(disabled.status.success() && disabled.stderr.is_empty());
        assert_eq!(disabled.stdout, first.stdout);
        fs::write(
            fixture.cache().join("stable-release-v1.json"),
            b"broken cache",
        )
        .unwrap_or_else(|e| panic!("fixture: {e}"));
        let broken = fixture.run(&args, "on", &fixture.cache());
        assert!(broken.status.success() && broken.stderr.is_empty());
        assert_eq!(broken.stdout, first.stdout);
    }
}

#[test]
fn explicit_status_distinguishes_client_server_and_upstream_without_notices() {
    let fixture = Fixture::new();
    fixture.seed();
    for action in ["version", "update-check"] {
        let result = fixture.run(
            &[action, "--destination", "fictional-knowledge"],
            "auto",
            &fixture.cache(),
        );
        assert!(result.status.success() && result.stderr.is_empty());
        let report: serde_json::Value =
            serde_json::from_slice(&result.stdout).unwrap_or_else(|e| panic!("JSON: {e}"));
        assert_eq!(report["client_update_available"], true);
        assert_eq!(report["server_update_available"], false);
        assert_eq!(report["server"]["protocol_matches"], true);
        assert_eq!(report["upstream"]["cached"], true);
        assert_eq!(report["upstream"]["latest"]["version"], "9.8.7");
    }
    let untouched = fixture.root.path().join("untouched");
    let disabled = fixture.run(
        &["update-check", "--destination", "fictional-knowledge"],
        "off",
        &untouched,
    );
    assert!(disabled.status.success() && disabled.stderr.is_empty());
    let report: serde_json::Value =
        serde_json::from_slice(&disabled.stdout).unwrap_or_else(|e| panic!("JSON: {e}"));
    assert_eq!(report["upstream"]["status"], "disabled");
    assert_eq!(report["server"]["status"], "available");
    assert!(report["client_update_available"].is_null());
    assert!(!untouched.exists());
    let worker = fixture.run(&["__update-cache"], "off", &untouched);
    assert!(worker.status.success() && worker.stdout.is_empty() && worker.stderr.is_empty());
    assert!(!untouched.exists());
}

#[test]
fn default_automation_and_plain_version_have_no_background_side_effects() {
    let fixture = Fixture::new();
    let cache = fixture.root.path().join("unused");
    for args in [
        vec!["list", "--destination", "fictional-knowledge"],
        vec!["--version"],
        vec!["version"],
    ] {
        let result = fixture.run(&args, "auto", &cache);
        assert!(result.status.success() && result.stderr.is_empty());
    }
    assert!(!cache.exists());
    for args in [
        vec!["version", "extra"],
        vec!["update-check", "--destination"],
        vec!["__update-cache", "extra"],
    ] {
        assert!(!fixture.run(&args, "off", &cache).status.success());
    }
}

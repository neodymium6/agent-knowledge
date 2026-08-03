use super::{CliError, Command, parse_arguments, run};
use std::ffi::OsString;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

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
        }
    ]
}"#;
const MARKDOWN: &str = "---\n\
schema_version: 1\n\
document_id: 01K00000000000000000000002\n\
title: Fictional benchmark\n\
created: 2026-07-31T03:50:00+09:00\n\
request_id: 01K00000000000000000000000\n\
tags:\n\
  - benchmark\n\
status: active\n\
---\n";

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

#[derive(Clone, Default)]
struct SharedOutput(Arc<Mutex<Vec<u8>>>);

impl SharedOutput {
    fn contents(&self) -> Vec<u8> {
        self.0
            .lock()
            .unwrap_or_else(|error| panic!("CLI output must not be poisoned: {error}"))
            .clone()
    }
}

impl Write for SharedOutput {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| io::Error::other("CLI output lock poisoned"))?
            .write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl TestDirectory {
    fn create() -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "agent-knowledge-cli-test-{}-{sequence}",
            std::process::id()
        ));
        if let Err(error) = fs::create_dir(&path) {
            panic!("CLI test directory must be created: {error}");
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
            && error.kind() != io::ErrorKind::NotFound
        {
            panic!("CLI test directory must be removed: {error}");
        }
    }
}

fn write_package(root: &Path) -> PathBuf {
    let package = root.join("package");
    if let Err(error) = fs::create_dir_all(package.join("payload/benchmark")) {
        panic!("package fixture directories must be created: {error}");
    }
    if let Err(error) = fs::write(package.join("request.json"), REQUEST_JSON) {
        panic!("request fixture must be written: {error}");
    }
    if let Err(error) = fs::write(package.join("payload/benchmark/index.md"), MARKDOWN) {
        panic!("Markdown fixture must be written: {error}");
    }
    package
}

fn arguments(root: &Path, package: &Path) -> Vec<OsString> {
    vec![
        "agent-knowledge".into(),
        "admin".into(),
        "submit".into(),
        "--queue-root".into(),
        root.join("queue").into_os_string(),
        "--package-root".into(),
        package.as_os_str().to_owned(),
    ]
}

#[test]
fn submits_valid_directory_and_reports_idempotent_retry() {
    let root = TestDirectory::create();
    let package = write_package(root.path());
    let first = SharedOutput::default();
    if let Err(error) = run(arguments(root.path(), &package), first.clone()) {
        panic!("first local submission must succeed: {error}");
    }
    let first: serde_json::Value = match serde_json::from_slice(&first.contents()) {
        Ok(response) => response,
        Err(error) => panic!("first response must be JSON: {error}"),
    };
    assert_eq!(first["status"], "accepted");
    assert_eq!(first["request_id"], "01K00000000000000000000000");
    assert!(
        root.path()
            .join("queue/pending/01K00000000000000000000000")
            .is_dir()
    );

    let second = SharedOutput::default();
    if let Err(error) = run(arguments(root.path(), &package), second.clone()) {
        panic!("idempotent local submission must succeed: {error}");
    }
    let second: serde_json::Value = match serde_json::from_slice(&second.contents()) {
        Ok(response) => response,
        Err(error) => panic!("second response must be JSON: {error}"),
    };
    assert_eq!(second["status"], "existing");
    assert_eq!(second["state"], "pending");
}

#[test]
fn rejects_unknown_or_incomplete_command_lines() {
    for arguments in [
        vec!["agent-knowledge".into()],
        vec!["agent-knowledge".into(), "admin".into(), "unknown".into()],
        vec!["agent-knowledge".into(), "worker".into(), "run".into()],
        vec![
            "agent-knowledge".into(),
            "admin".into(),
            "submit".into(),
            "--queue-root".into(),
            "fictional-queue".into(),
        ],
    ] {
        assert!(matches!(run(arguments, Vec::new()), Err(CliError::Usage)));
    }
}

#[test]
fn parses_the_worker_configuration_command() {
    let command = parse_arguments([
        "agent-knowledge".into(),
        "worker".into(),
        "run".into(),
        "--config".into(),
        "/srv/fictional-knowledge/worker.yaml".into(),
    ])
    .unwrap_or_else(|error| panic!("Worker command must parse: {error}"));

    assert!(matches!(
        command,
        Command::RunWorker { config }
            if config == Path::new("/srv/fictional-knowledge/worker.yaml")
    ));
}

#[test]
fn parses_and_bounds_the_local_operational_status_command() {
    let command = parse_arguments([
        "agent-knowledge".into(),
        "admin".into(),
        "status".into(),
        "--config".into(),
        "/srv/fictional-knowledge/worker.yaml".into(),
        "--maximum-queue-entries".into(),
        "4242".into(),
        "--timeout-seconds".into(),
        "42".into(),
    ])
    .unwrap_or_else(|error| panic!("admin status command must parse: {error}"));

    assert!(matches!(
        command,
        Command::AdminStatus { config, maximum_queue_entries: 4242, timeout }
            if config == Path::new("/srv/fictional-knowledge/worker.yaml")
                && timeout == std::time::Duration::from_secs(42)
    ));

    for (flag, value) in [
        ("--maximum-queue-entries", "0"),
        ("--maximum-queue-entries", "1000001"),
        ("--timeout-seconds", "0"),
        ("--timeout-seconds", "301"),
    ] {
        assert!(matches!(
            parse_arguments([
                "agent-knowledge".into(),
                "admin".into(),
                "status".into(),
                "--config".into(),
                "/srv/fictional-knowledge/worker.yaml".into(),
                flag.into(),
                value.into(),
            ]),
            Err(CliError::Usage)
        ));
    }
}

#[test]
fn parses_and_bounds_the_release_retention_command() {
    let command = parse_arguments([
        "agent-knowledge".into(),
        "admin".into(),
        "prune-releases".into(),
        "--config".into(),
        "/srv/fictional-knowledge/worker.yaml".into(),
        "--dry-run".into(),
        "--timeout-seconds".into(),
        "900".into(),
    ])
    .unwrap_or_else(|error| panic!("release retention command must parse: {error}"));

    assert!(matches!(
        command,
        Command::AdminPruneReleases { config, dry_run: true, timeout }
            if config == Path::new("/srv/fictional-knowledge/worker.yaml")
                && timeout == std::time::Duration::from_secs(900)
    ));

    for arguments in [
        vec![
            "agent-knowledge".into(),
            "admin".into(),
            "prune-releases".into(),
            "--config".into(),
            "/srv/fictional-knowledge/worker.yaml".into(),
            "--dry-run".into(),
            "--dry-run".into(),
        ],
        vec![
            "agent-knowledge".into(),
            "admin".into(),
            "prune-releases".into(),
            "--config".into(),
            "/srv/fictional-knowledge/worker.yaml".into(),
            "--timeout-seconds".into(),
            "3601".into(),
        ],
    ] {
        assert!(matches!(parse_arguments(arguments), Err(CliError::Usage)));
    }
}

#[cfg(target_os = "linux")]
#[test]
fn parses_the_descriptor_relative_storage_migration_command() {
    let command = parse_arguments([
        "agent-knowledge".into(),
        "admin".into(),
        "migrate-v1-storage".into(),
        "--queue-root".into(),
        "/srv/fictional-queue".into(),
        "--git-directory".into(),
        "/srv/fictional-repository".into(),
        "--content-root".into(),
        "/srv/fictional-content".into(),
        "--queue-owner".into(),
        "61101".into(),
        "--queue-group".into(),
        "61102".into(),
        "--gateway-group".into(),
        "61103".into(),
    ])
    .unwrap_or_else(|error| panic!("storage migration command must parse: {error}"));

    assert!(matches!(
        command,
        Command::AdminMigrateV1Storage {
            queue_root,
            git_directory,
            content_root,
            queue_owner,
            queue_group,
            gateway_group,
        } if queue_root == Path::new("/srv/fictional-queue")
            && git_directory == Path::new("/srv/fictional-repository")
            && content_root == Path::new("/srv/fictional-content")
            && queue_owner == "61101"
            && queue_group == "61102"
            && gateway_group == "61103"
    ));
}

#[cfg(target_os = "linux")]
#[test]
fn parses_the_storage_bootstrap_command() {
    let command = parse_arguments([
        "agent-knowledge".into(),
        "admin".into(),
        "bootstrap-storage".into(),
        "--config".into(),
        "/etc/fictional-knowledge/worker.yaml".into(),
        "--runtime-directory".into(),
        "/run/fictional-knowledge".into(),
        "--worker-owner".into(),
        "61001".into(),
        "--worker-group".into(),
        "61002".into(),
        "--queue-owner".into(),
        "61003".into(),
        "--queue-group".into(),
        "61004".into(),
        "--gateway-owner".into(),
        "61005".into(),
        "--gateway-group".into(),
        "61006".into(),
        "--ingress-group".into(),
        "61007".into(),
    ])
    .unwrap_or_else(|error| panic!("storage bootstrap command must parse: {error}"));

    assert!(matches!(
        command,
        Command::AdminBootstrapStorage(settings)
            if settings.config == Path::new("/etc/fictional-knowledge/worker.yaml")
                && settings.runtime_directory == Path::new("/run/fictional-knowledge")
                && settings.worker_owner == "61001"
                && settings.worker_group == "61002"
                && settings.queue_owner == "61003"
                && settings.queue_group == "61004"
                && settings.gateway_owner == "61005"
                && settings.gateway_group == "61006"
                && settings.ingress_group == "61007"
    ));

    assert!(matches!(
        parse_arguments([
            "agent-knowledge".into(),
            "admin".into(),
            "bootstrap-storage".into(),
            "--config".into(),
            "/etc/fictional-knowledge/worker.yaml".into(),
        ]),
        Err(CliError::Usage)
    ));
}

#[test]
fn parses_the_forced_command_gateway_configuration() {
    let command = parse_arguments([
        "agent-knowledge".into(),
        "gateway".into(),
        "--config".into(),
        "/srv/fictional-knowledge/gateway.yaml".into(),
        "--client-id".into(),
        "fictional-node-a".into(),
    ])
    .unwrap_or_else(|error| panic!("Gateway command must parse: {error}"));

    assert!(matches!(
        command,
        Command::RunGateway { config, client_id }
            if config == Path::new("/srv/fictional-knowledge/gateway.yaml")
                && client_id == "fictional-node-a"
    ));
}

#[test]
fn parses_the_systemd_activated_queue_ingress_command() {
    let command = parse_arguments([
        "agent-knowledge".into(),
        "queue-ingress".into(),
        "serve".into(),
        "--queue-root".into(),
        "/srv/fictional-knowledge/queue".into(),
        "--socket-path".into(),
        "/run/fictional-knowledge/queue-ingress.sock".into(),
    ])
    .unwrap_or_else(|error| panic!("queue ingress command must parse: {error}"));

    assert!(matches!(
        command,
        Command::ServeQueueIngress { queue_root, socket_path }
            if queue_root == Path::new("/srv/fictional-knowledge/queue")
                && socket_path == Path::new("/run/fictional-knowledge/queue-ingress.sock")
    ));
}

#[test]
fn rejects_systemd_activated_queue_ingress_without_its_socket_path() {
    assert!(matches!(
        parse_arguments([
            "agent-knowledge".into(),
            "queue-ingress".into(),
            "serve".into(),
            "--queue-root".into(),
            "/srv/fictional-knowledge/queue".into(),
        ]),
        Err(CliError::Usage)
    ));
}

#[test]
fn parses_the_long_running_queue_ingress_command() {
    let command = parse_arguments([
        "agent-knowledge".into(),
        "queue-ingress".into(),
        "listen".into(),
        "--queue-root".into(),
        "/srv/fictional-knowledge/queue".into(),
        "--socket-path".into(),
        "/run/fictional-knowledge/queue-ingress.sock".into(),
        "--maximum-connections".into(),
        "12".into(),
        "--connection-timeout-seconds".into(),
        "900".into(),
    ])
    .unwrap_or_else(|error| panic!("queue ingress listener command must parse: {error}"));

    assert!(matches!(
        command,
        Command::ListenQueueIngress { settings }
            if settings.queue_root == Path::new("/srv/fictional-knowledge/queue")
                && settings.socket_path
                    == Path::new("/run/fictional-knowledge/queue-ingress.sock")
                && settings.maximum_connections.get() == 12
                && settings.connection_timeout == std::time::Duration::from_secs(900)
    ));
}

#[test]
fn parses_the_ssh_client_submission_command() {
    let command = parse_arguments([
        "agent-knowledge".into(),
        "client".into(),
        "submit".into(),
        "--destination".into(),
        "fictional-knowledge".into(),
        "--package-root".into(),
        "/tmp/fictional-package".into(),
    ])
    .unwrap_or_else(|error| panic!("client submit command must parse: {error}"));

    assert!(matches!(
        command,
        Command::ClientSubmit { destination, package_root, timeout }
            if destination == "fictional-knowledge"
                && package_root == Path::new("/tmp/fictional-package")
                && timeout == std::time::Duration::from_secs(300)
    ));
}

#[test]
fn parses_committed_read_and_search_commands() {
    let list = parse_arguments([
        "agent-knowledge".into(),
        "client".into(),
        "recent".into(),
        "--destination".into(),
        "fictional-knowledge".into(),
        "--project".into(),
        "fictional-project".into(),
        "--tag".into(),
        "operations".into(),
        "--include-archived".into(),
        "--maximum-results".into(),
        "25".into(),
    ])
    .unwrap_or_else(|error| panic!("client recent command must parse: {error}"));
    assert!(matches!(
        list,
        Command::ClientList { destination, request, recent: true, timeout }
            if destination == "fictional-knowledge"
                && request.filter.project.is_some()
                && request.filter.tag.as_deref() == Some("operations")
                && request.filter.include_archived
                && request.maximum_results == 25
                && timeout == std::time::Duration::from_secs(300)
    ));

    let search = parse_arguments([
        "agent-knowledge".into(),
        "client".into(),
        "search".into(),
        "--destination".into(),
        "fictional-knowledge".into(),
        "--query".into(),
        "restart procedure".into(),
    ])
    .unwrap_or_else(|error| panic!("client search command must parse: {error}"));
    assert!(matches!(
        search,
        Command::ClientSearch { request, .. }
            if request.query == "restart procedure" && request.maximum_results == 100
    ));

    let get = parse_arguments([
        "agent-knowledge".into(),
        "client".into(),
        "get".into(),
        "--destination".into(),
        "fictional-knowledge".into(),
        "--document-id".into(),
        "01K00000000000000000000001".into(),
    ])
    .unwrap_or_else(|error| panic!("client get command must parse: {error}"));
    assert!(matches!(get, Command::ClientGet { .. }));

    let export = parse_arguments([
        "agent-knowledge".into(),
        "client".into(),
        "export".into(),
        "--destination".into(),
        "fictional-knowledge".into(),
        "--document-id".into(),
        "01K00000000000000000000001".into(),
    ])
    .unwrap_or_else(|error| panic!("client export command must parse: {error}"));
    assert!(matches!(export, Command::ClientExport { .. }));
}

#[test]
fn parses_request_status_command() {
    let command = parse_arguments([
        "agent-knowledge".into(),
        "client".into(),
        "status".into(),
        "--destination".into(),
        "fictional-knowledge".into(),
        "--request-id".into(),
        "01K00000000000000000000000".into(),
        "--timeout-seconds".into(),
        "42".into(),
    ])
    .unwrap_or_else(|error| panic!("client status command must parse: {error}"));
    assert!(matches!(
        command,
        Command::ClientStatus { destination, request, timeout }
            if destination == "fictional-knowledge"
                && request.request_id.to_string() == "01K00000000000000000000000"
                && timeout == std::time::Duration::from_secs(42)
    ));

    for arguments in [
        vec![
            "agent-knowledge".into(),
            "client".into(),
            "status".into(),
            "--destination".into(),
            "fictional-knowledge".into(),
        ],
        vec![
            "agent-knowledge".into(),
            "client".into(),
            "status".into(),
            "--destination".into(),
            "fictional-knowledge".into(),
            "--request-id".into(),
            "invalid".into(),
        ],
    ] {
        assert!(matches!(parse_arguments(arguments), Err(CliError::Usage)));
    }
}

#[test]
fn rejects_invalid_committed_read_arguments() {
    for arguments in [
        vec![
            "agent-knowledge".into(),
            "client".into(),
            "search".into(),
            "--destination".into(),
            "fictional-knowledge".into(),
        ],
        vec![
            "agent-knowledge".into(),
            "client".into(),
            "list".into(),
            "--destination".into(),
            "fictional-knowledge".into(),
            "--maximum-results".into(),
            "0".into(),
        ],
        vec![
            "agent-knowledge".into(),
            "client".into(),
            "get".into(),
            "--destination".into(),
            "fictional-knowledge".into(),
            "--document-id".into(),
            "invalid".into(),
        ],
    ] {
        assert!(matches!(parse_arguments(arguments), Err(CliError::Usage)));
    }
}

#[test]
fn parses_and_bounds_the_ssh_client_timeout() {
    let command = parse_arguments([
        "agent-knowledge".into(),
        "client".into(),
        "submit".into(),
        "--destination".into(),
        "fictional-knowledge".into(),
        "--package-root".into(),
        "/tmp/fictional-package".into(),
        "--timeout-seconds".into(),
        "42".into(),
    ])
    .unwrap_or_else(|error| panic!("client timeout must parse: {error}"));
    assert!(matches!(
        command,
        Command::ClientSubmit { timeout, .. }
            if timeout == std::time::Duration::from_secs(42)
    ));

    for timeout in ["0", "3601", "not-a-number"] {
        assert!(matches!(
            parse_arguments([
                "agent-knowledge".into(),
                "client".into(),
                "submit".into(),
                "--destination".into(),
                "fictional-knowledge".into(),
                "--package-root".into(),
                "/tmp/fictional-package".into(),
                "--timeout-seconds".into(),
                timeout.into(),
            ]),
            Err(CliError::Usage)
        ));
    }
}

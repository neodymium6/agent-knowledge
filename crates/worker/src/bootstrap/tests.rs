use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration as StandardDuration, Instant};

use time::{Duration, OffsetDateTime};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use super::{WorkerBootstrap, WorkerOpenError};
use crate::status::inspect_operational_status_with_hook;
use crate::{
    OperationalStatusError, RemoteReplicationOutcome, ReplicationStatus, StartupOutcome,
    WorkerSettings, inspect_operational_status,
};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn create() -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "agent-knowledge-worker-bootstrap-test-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap_or_else(|error| panic!("test root must be created: {error}"));
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.0)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            panic!("test root must be removed: {error}");
        }
    }
}

#[test]
fn opens_components_and_completes_clean_startup_recovery() {
    let root = TestDirectory::create();
    initialize_repository(root.path());
    initialize_quartz(root.path());
    let settings = WorkerSettings::decode(&valid_yaml(root.path()))
        .unwrap_or_else(|error| panic!("fixture settings must decode: {error}"));
    let bootstrap = WorkerBootstrap::open(settings)
        .unwrap_or_else(|error| panic!("configured components must open: {error}"));
    let (_runtime, startup, schedule) = bootstrap
        .start(created_at())
        .unwrap_or_else(|error| panic!("configured Worker must start: {error}"));

    assert_eq!(startup, StartupOutcome::Clean);
    assert_eq!(schedule.debounce(), Duration::seconds(30));
}

#[test]
fn inspects_initialized_components_without_starting_the_worker() {
    let root = TestDirectory::create();
    initialize_repository(root.path());
    initialize_quartz(root.path());
    let settings = WorkerSettings::decode(&valid_yaml(root.path()))
        .unwrap_or_else(|error| panic!("fixture settings must decode: {error}"));
    let _bootstrap = WorkerBootstrap::open(settings.clone())
        .unwrap_or_else(|error| panic!("configured components must open: {error}"));

    let status = inspect_operational_status(
        &settings,
        1024,
        Some(Instant::now() + StandardDuration::from_secs(5)),
        created_at(),
    )
    .unwrap_or_else(|error| panic!("operational status must be readable: {error}"));

    assert_eq!(status.queue().pending(), 0);
    assert_eq!(status.queue().processing(), 0);
    assert_eq!(status.queue().completed(), 0);
    assert_eq!(status.queue().failed(), 0);
    assert_eq!(status.queue().oldest_pending_at(), None);
    assert!(!status.queue().worker_active());
    assert!(!status.queue().snapshot_exact());
    assert!(status.publication().active_release().is_none());
    assert!(!status.publication().synchronized());
    assert!(matches!(status.replication(), ReplicationStatus::Disabled));
    let wire = serde_json::to_value(&status)
        .unwrap_or_else(|error| panic!("operational status must serialize: {error}"));
    assert_eq!(wire["schema_version"], 1);
    assert_eq!(wire["observed_at"], "2026-07-31T04:00:00Z");
    assert_eq!(wire["queue"]["worker_active"], false);
    assert_eq!(wire["queue"]["snapshot_exact"], false);
    assert_eq!(
        wire["publication"]["active_release"],
        serde_json::Value::Null
    );
    assert_eq!(wire["replication"]["status"], "disabled");
}

#[test]
fn operational_status_rejects_a_publication_changed_during_observation() {
    let root = TestDirectory::create();
    initialize_repository(root.path());
    initialize_quartz(root.path());
    let settings = WorkerSettings::decode(&valid_yaml(root.path()))
        .unwrap_or_else(|error| panic!("fixture settings must decode: {error}"));
    let _bootstrap = WorkerBootstrap::open(settings.clone())
        .unwrap_or_else(|error| panic!("configured components must open: {error}"));

    let result = inspect_operational_status_with_hook(
        &settings,
        1024,
        Some(Instant::now() + StandardDuration::from_secs(5)),
        created_at(),
        || {
            run_git(
                Some(&root.path().join("content")),
                [
                    "-c",
                    "user.name=Fictional Test Author",
                    "-c",
                    "user.email=worker@example.invalid",
                    "commit",
                    "--allow-empty",
                    "-m",
                    "Advance fictional publication",
                ],
                None,
            );
        },
    );

    assert!(matches!(
        result,
        Err(OperationalStatusError::PublicationChanged)
    ));
}

#[test]
fn invalid_repository_fails_before_queue_initialization() {
    let root = TestDirectory::create();
    initialize_quartz(root.path());
    let settings = WorkerSettings::decode(&valid_yaml(root.path()))
        .unwrap_or_else(|error| panic!("fixture settings must decode: {error}"));

    assert!(matches!(
        WorkerBootstrap::open(settings),
        Err(WorkerOpenError::Repository(_))
    ));
    assert!(!root.path().join("queue").exists());
}

#[test]
fn configured_replication_requires_an_existing_repository_remote() {
    let root = TestDirectory::create();
    initialize_repository(root.path());
    initialize_quartz(root.path());
    let yaml = valid_yaml(root.path()).replace(
        "  author_email: worker@example.invalid\n",
        "  author_email: worker@example.invalid\n  replication:\n    remote: fictional-backup\n    branch: main\n    timeout_seconds: 5\n    initial_backoff_seconds: 10\n    maximum_backoff_seconds: 40\n",
    );
    let settings = WorkerSettings::decode(&yaml)
        .unwrap_or_else(|error| panic!("fixture settings must decode: {error}"));

    assert!(matches!(
        WorkerBootstrap::open(settings),
        Err(WorkerOpenError::Replication(_))
    ));
    assert!(!root.path().join("queue").exists());
}

#[test]
fn configured_replication_runs_in_the_background_after_startup() {
    let root = TestDirectory::create();
    initialize_repository(root.path());
    initialize_quartz(root.path());
    let backup = root.path().join("fictional-backup");
    run_git(
        None,
        ["init", "--bare", "--initial-branch=main"],
        Some(&backup),
    );
    run_git(
        Some(&root.path().join("repository")),
        ["remote", "add", "fictional-backup"],
        Some(&backup),
    );
    let yaml = valid_yaml(root.path()).replace(
        "  author_email: worker@example.invalid\n",
        "  author_email: worker@example.invalid\n  replication:\n    remote: fictional-backup\n    branch: main\n    timeout_seconds: 5\n    initial_backoff_seconds: 1\n    maximum_backoff_seconds: 4\n",
    );
    let settings = WorkerSettings::decode(&yaml)
        .unwrap_or_else(|error| panic!("fixture settings must decode: {error}"));
    let bootstrap = WorkerBootstrap::open(settings)
        .unwrap_or_else(|error| panic!("configured components must open: {error}"));
    let (runtime, startup, _) = bootstrap
        .start(created_at())
        .unwrap_or_else(|error| panic!("configured Worker must start: {error}"));

    assert_eq!(startup, StartupOutcome::Clean);
    let deadline = Instant::now() + StandardDuration::from_secs(5);
    let outcome = loop {
        if let Some(outcome) = runtime.take_replication_event() {
            break outcome;
        }
        assert!(
            Instant::now() < deadline,
            "background replication must report its initial push"
        );
        thread::sleep(StandardDuration::from_millis(10));
    };
    assert!(matches!(
        outcome,
        Ok(RemoteReplicationOutcome::Pushed { .. })
    ));
}

#[test]
fn background_replication_preserves_events_while_the_receiver_is_delayed() {
    let root = TestDirectory::create();
    initialize_repository(root.path());
    initialize_quartz(root.path());
    let backup = root.path().join("fictional-backup");
    run_git(
        None,
        ["init", "--bare", "--initial-branch=main"],
        Some(&backup),
    );
    run_git(
        Some(&root.path().join("repository")),
        ["remote", "add", "fictional-backup"],
        Some(&backup),
    );
    let yaml = valid_yaml(root.path()).replace(
        "  author_email: worker@example.invalid\n",
        "  author_email: worker@example.invalid\n  replication:\n    remote: fictional-backup\n    branch: main\n    timeout_seconds: 5\n    initial_backoff_seconds: 1\n    maximum_backoff_seconds: 4\n",
    );
    let settings = WorkerSettings::decode(&yaml)
        .unwrap_or_else(|error| panic!("fixture settings must decode: {error}"));
    let bootstrap = WorkerBootstrap::open(settings)
        .unwrap_or_else(|error| panic!("configured components must open: {error}"));
    fs::remove_dir_all(&backup)
        .unwrap_or_else(|error| panic!("backup outage must be simulated: {error}"));
    let (runtime, _, _) = bootstrap
        .start(created_at())
        .unwrap_or_else(|error| panic!("configured Worker must start: {error}"));
    let state = root
        .path()
        .join("repository/agent-knowledge/remote-replication-v1.json");
    wait_until("failed replication state", || state.exists());

    run_git(
        None,
        ["init", "--bare", "--initial-branch=main"],
        Some(&backup),
    );
    wait_until("replicated backup ref", || {
        Command::new("git")
            .arg(format!("--git-dir={}", backup.display()))
            .args(["rev-parse", "--verify", "refs/heads/main"])
            .output()
            .is_ok_and(|output| output.status.success())
    });

    let deadline = Instant::now() + StandardDuration::from_secs(5);
    let mut outcomes = Vec::new();
    while outcomes.len() < 2 {
        if let Some(outcome) = runtime.take_replication_event() {
            outcomes.push(outcome);
            continue;
        }
        assert!(
            Instant::now() < deadline,
            "both replication events must remain available"
        );
        thread::sleep(StandardDuration::from_millis(10));
    }
    assert!(matches!(
        outcomes.first(),
        Some(Ok(RemoteReplicationOutcome::Failed { .. }))
    ));
    assert!(matches!(
        outcomes.get(1),
        Some(Ok(RemoteReplicationOutcome::Pushed { .. }))
    ));
}

#[cfg(unix)]
#[test]
fn rejects_resolved_storage_aliases_before_initialization() {
    use std::os::unix::fs::symlink;

    let root = TestDirectory::create();
    initialize_repository(root.path());
    initialize_quartz(root.path());
    let alias = root.path().join("aliased-storage");
    symlink(root.path().join("content"), &alias)
        .unwrap_or_else(|error| panic!("storage alias must be created: {error}"));
    let yaml = valid_yaml(root.path()).replace(
        &root.path().join("queue").display().to_string(),
        &alias.join("queue").display().to_string(),
    );
    let settings = WorkerSettings::decode(&yaml)
        .unwrap_or_else(|error| panic!("lexically distinct settings must decode: {error}"));

    assert!(matches!(
        WorkerBootstrap::open(settings),
        Err(WorkerOpenError::OverlappingPaths { .. })
    ));
    assert!(!root.path().join("content/queue").exists());
    assert!(!root.path().join("releases").exists());
}

#[cfg(unix)]
#[test]
fn initialization_uses_the_destination_resolved_during_preflight() {
    use std::os::unix::fs::symlink;

    let root = TestDirectory::create();
    initialize_repository(root.path());
    initialize_quartz(root.path());
    let safe_storage = root.path().join("safe-storage");
    fs::create_dir(&safe_storage)
        .unwrap_or_else(|error| panic!("safe storage must be created: {error}"));
    let alias = root.path().join("queue-alias");
    symlink(&safe_storage, &alias)
        .unwrap_or_else(|error| panic!("queue alias must be created: {error}"));
    let yaml = valid_yaml(root.path()).replace(
        &root.path().join("queue").display().to_string(),
        &alias.join("queue").display().to_string(),
    );
    let settings = WorkerSettings::decode(&yaml)
        .unwrap_or_else(|error| panic!("fixture settings must decode: {error}"));

    WorkerBootstrap::open_with_preflight_hook(settings, || {
        fs::remove_file(&alias)
            .unwrap_or_else(|error| panic!("original queue alias must be removed: {error}"));
        symlink(root.path().join("content"), &alias)
            .unwrap_or_else(|error| panic!("replacement queue alias must be created: {error}"));
    })
    .unwrap_or_else(|error| panic!("resolved configuration must open: {error}"));

    assert!(safe_storage.join("queue").is_dir());
    assert!(!root.path().join("content/queue").exists());
}

fn valid_yaml(root: &Path) -> String {
    format!(
        r#"schema_version: 1
storage:
  queue_root: {queue}
  repository_root: {repository}
  content_root: {content}
  work_root: {work}
  release_root: {releases}
repository:
  official_branch: main
  author_name: Fictional Knowledge Worker
  author_email: worker@example.invalid
quartz:
  program: {script}
  integration_root: {integration}
  timeout_seconds: 5
batch:
  debounce_seconds: 30
  maximum_age_seconds: 300
  maximum_scan_entries: 1024
  maximum_requests: 100
  maximum_recovery_requests: 10000
"#,
        queue = root.join("queue").display(),
        repository = root.join("repository").display(),
        content = root.join("content").display(),
        work = root.join("work").display(),
        releases = root.join("releases").display(),
        integration = root.join("quartz-integration").display(),
        script = root.join("quartz-integration/quartz.sh").display(),
    )
}

fn initialize_repository(root: &Path) {
    let repository = root.join("repository");
    let seed = root.join("seed");
    let content = root.join("content");
    run_git(
        None,
        ["init", "--bare", "--initial-branch=main"],
        Some(&repository),
    );
    run_git(None, ["init", "--initial-branch=main"], Some(&seed));
    run_git(
        Some(&seed),
        ["config", "user.name", "Fictional Test Author"],
        None,
    );
    run_git(
        Some(&seed),
        ["config", "user.email", "worker@example.invalid"],
        None,
    );
    run_git(
        Some(&seed),
        [
            "commit",
            "--allow-empty",
            "-m",
            "Initialize fictional knowledge",
        ],
        None,
    );
    run_git(Some(&seed), ["remote", "add", "origin"], Some(&repository));
    run_git(Some(&seed), ["push", "origin", "main"], None);
    let status = Command::new("git")
        .arg(format!("--git-dir={}", repository.display()))
        .args(["worktree", "add"])
        .arg(content)
        .arg("main")
        .status()
        .unwrap_or_else(|error| panic!("canonical worktree command must run: {error}"));
    assert!(status.success());
}

fn initialize_quartz(root: &Path) {
    let integration = root.join("quartz-integration");
    fs::create_dir(&integration)
        .unwrap_or_else(|error| panic!("Quartz integration must be created: {error}"));
    let script = integration.join("quartz.sh");
    fs::write(
        &script,
        b"#!/bin/sh\nprintf '%s\\n' '<p>fictional site</p>' > \"$5/index.html\"\n",
    )
    .unwrap_or_else(|error| panic!("Quartz fixture must be written: {error}"));
    #[cfg(unix)]
    {
        let mut permissions = fs::metadata(&script)
            .unwrap_or_else(|error| panic!("Quartz fixture metadata must be read: {error}"))
            .permissions();
        permissions.set_mode(0o500);
        fs::set_permissions(script, permissions)
            .unwrap_or_else(|error| panic!("Quartz fixture must be executable: {error}"));
    }
}

fn run_git<const N: usize>(working: Option<&Path>, arguments: [&str; N], path: Option<&Path>) {
    let mut command = Command::new("git");
    if let Some(working) = working {
        command.current_dir(working);
    }
    command.args(arguments);
    if let Some(path) = path {
        command.arg(path);
    }
    let status = command
        .status()
        .unwrap_or_else(|error| panic!("Git fixture command must run: {error}"));
    assert!(status.success());
}

fn wait_until(description: &str, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + StandardDuration::from_secs(5);
    while !condition() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {description}"
        );
        thread::sleep(StandardDuration::from_millis(10));
    }
}

fn created_at() -> OffsetDateTime {
    OffsetDateTime::parse(
        "2026-07-31T04:00:00Z",
        &time::format_description::well_known::Rfc3339,
    )
    .unwrap_or_else(|error| panic!("timestamp must parse: {error}"))
}

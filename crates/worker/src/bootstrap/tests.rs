use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

use time::{Duration, OffsetDateTime};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use super::{WorkerBootstrap, WorkerOpenError};
use crate::{StartupOutcome, WorkerSettings};

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

fn created_at() -> OffsetDateTime {
    OffsetDateTime::parse(
        "2026-07-31T04:00:00Z",
        &time::format_description::well_known::Rfc3339,
    )
    .unwrap_or_else(|error| panic!("timestamp must parse: {error}"))
}

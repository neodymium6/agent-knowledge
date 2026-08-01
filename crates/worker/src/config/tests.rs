use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use time::Duration;

use super::{MAXIMUM_WORKER_CONFIG_BYTES, WorkerConfigError, WorkerSettings};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn create() -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "agent-knowledge-worker-config-test-{}-{sequence}",
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

#[test]
fn decodes_strict_versioned_operational_settings() {
    let settings = WorkerSettings::decode(&valid_yaml(Path::new("/srv/fictional-knowledge")))
        .unwrap_or_else(|error| panic!("fixture settings must decode: {error}"));

    assert_eq!(settings.schedule().debounce(), Duration::seconds(30));
    assert_eq!(settings.schedule().maximum_age(), Duration::minutes(5));
    assert_eq!(settings.limits().maximum_scan_entries().get(), 1024);
    assert_eq!(settings.limits().maximum_requests().get(), 100);
    assert_eq!(settings.limits().maximum_recovery_requests().get(), 10_000);
}

#[test]
fn rejects_unknown_fields_aliases_and_multiple_documents() {
    let root = Path::new("/srv/fictional-knowledge");
    let mut unknown = valid_yaml(root);
    unknown.push_str("unknown: true\n");
    assert!(matches!(
        WorkerSettings::decode(&unknown),
        Err(WorkerConfigError::InvalidYaml(_))
    ));

    let free_form_arguments = valid_yaml(root).replace(
        "  integration_root: /srv/fictional-knowledge/quartz-integration\n",
        "  integration_root: /srv/fictional-knowledge/quartz-integration\n  arguments:\n    - quartz\n",
    );
    assert!(matches!(
        WorkerSettings::decode(&free_form_arguments),
        Err(WorkerConfigError::InvalidYaml(_))
    ));

    let aliases = valid_yaml(root).replace(
        "  author_name: Fictional Knowledge Worker\n  author_email: worker@example.invalid",
        "  author_name: &identity Fictional Knowledge Worker\n  author_email: *identity",
    );
    assert!(matches!(
        WorkerSettings::decode(&aliases),
        Err(WorkerConfigError::InvalidYaml(_))
    ));

    let multiple = format!("{}---\n{}", valid_yaml(root), valid_yaml(root));
    assert!(matches!(
        WorkerSettings::decode(&multiple),
        Err(WorkerConfigError::InvalidYaml(_))
    ));
}

#[test]
fn rejects_unsupported_versions_and_oversized_input() {
    let root = Path::new("/srv/fictional-knowledge");
    let unsupported = valid_yaml(root).replace("schema_version: 1", "schema_version: 2");
    assert!(matches!(
        WorkerSettings::decode(&unsupported),
        Err(WorkerConfigError::UnsupportedSchemaVersion { found: 2 })
    ));

    let oversized = "x".repeat(MAXIMUM_WORKER_CONFIG_BYTES as usize + 1);
    assert!(matches!(
        WorkerSettings::decode(&oversized),
        Err(WorkerConfigError::FileTooLarge { .. })
    ));
}

#[test]
fn rejects_unsafe_paths_and_invalid_operational_bounds() {
    let root = Path::new("/srv/fictional-knowledge");
    let relative =
        valid_yaml(root).replace("/srv/fictional-knowledge/queue", "relative/fictional-queue");
    assert!(matches!(
        WorkerSettings::decode(&relative),
        Err(WorkerConfigError::InvalidPath {
            field: "storage.queue_root"
        })
    ));

    let overlapping = valid_yaml(root).replace(
        "/srv/fictional-knowledge/releases",
        "/srv/fictional-knowledge/work/releases",
    );
    assert!(matches!(
        WorkerSettings::decode(&overlapping),
        Err(WorkerConfigError::OverlappingPaths { .. })
    ));

    let mutable_quartz = valid_yaml(root).replace(
        "/srv/fictional-knowledge/quartz-integration",
        "/srv/fictional-knowledge/content/quartz-integration",
    );
    assert!(matches!(
        WorkerSettings::decode(&mutable_quartz),
        Err(WorkerConfigError::OverlappingPaths { .. })
    ));

    let zero_limit = valid_yaml(root).replace("maximum_requests: 100", "maximum_requests: 0");
    assert!(matches!(
        WorkerSettings::decode(&zero_limit),
        Err(WorkerConfigError::InvalidValue {
            field: "batch.maximum_requests"
        })
    ));

    let insufficient_recovery = valid_yaml(root).replace(
        "maximum_recovery_requests: 10000",
        "maximum_recovery_requests: 99",
    );
    assert!(matches!(
        WorkerSettings::decode(&insufficient_recovery),
        Err(WorkerConfigError::InvalidValue {
            field: "batch.maximum_recovery_requests"
        })
    ));

    let excessive_timeout = valid_yaml(root).replace(
        "timeout_seconds: 5",
        "timeout_seconds: 18446744073709551615",
    );
    assert!(matches!(
        WorkerSettings::decode(&excessive_timeout),
        Err(WorkerConfigError::InvalidValue {
            field: "quartz.timeout_seconds"
        })
    ));
}

#[test]
#[cfg(unix)]
fn load_pins_a_regular_projected_configuration_target() {
    use std::os::unix::fs::symlink;

    let root = TestDirectory::create();
    let target = root.path().join("worker-target.yaml");
    let link = root.path().join("worker.yaml");
    fs::write(&target, valid_yaml(root.path()))
        .unwrap_or_else(|error| panic!("configuration fixture must be written: {error}"));
    symlink(&target, &link)
        .unwrap_or_else(|error| panic!("configuration symlink must be created: {error}"));

    let settings = WorkerSettings::load(&link)
        .unwrap_or_else(|error| panic!("projected configuration must load: {error}"));
    assert_eq!(settings.schedule().debounce(), Duration::seconds(30));
}

#[test]
#[cfg(unix)]
fn load_rejects_a_fifo_without_blocking() {
    use nix::sys::stat::Mode;
    use nix::unistd::mkfifo;

    let root = TestDirectory::create();
    let fifo = root.path().join("worker.yaml");
    mkfifo(&fifo, Mode::S_IRUSR | Mode::S_IWUSR)
        .unwrap_or_else(|error| panic!("configuration FIFO must be created: {error}"));

    assert!(matches!(
        WorkerSettings::load(&fifo),
        Err(WorkerConfigError::InvalidFileType)
    ));
}

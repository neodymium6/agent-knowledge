use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use nix::unistd::{Gid, Uid};
use ulid::Ulid;

use super::{
    StorageBootstrap, StorageBootstrapError, StorageIdentities, bootstrap_storage_with_ids,
};

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "agent-knowledge-storage-bootstrap-test-{}",
            Ulid::generate()
        ));
        fs::create_dir(&path)
            .unwrap_or_else(|error| panic!("test directory must be created: {error}"));
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.path) {
            eprintln!("failed to remove storage bootstrap test directory: {error}");
        }
    }
}

fn fixture(root: &Path) -> (StorageBootstrap, StorageIdentities) {
    let storage = root.join("storage");
    let runtime_parent = root.join("run");
    fs::create_dir(&runtime_parent)
        .unwrap_or_else(|error| panic!("runtime parent must be created: {error}"));
    let config = root.join("worker.yaml");
    fs::write(
        &config,
        format!(
            "schema_version: 1\nstorage:\n  queue_root: {0}/queue\n  repository_root: {0}/repository\n  content_root: {0}/content\n  work_root: {0}/work\n  release_root: {0}/releases\nrepository:\n  official_branch: main\n  author_name: Fictional Knowledge Worker\n  author_email: worker@example.invalid\nquartz:\n  program: /opt/fictional-quartz/bin/build-site\n  integration_root: /opt/fictional-quartz\n  timeout_seconds: 300\nbatch:\n  debounce_seconds: 30\n  maximum_age_seconds: 300\n  maximum_scan_entries: 1024\n  maximum_requests: 100\n  maximum_recovery_requests: 10000\n",
            storage.display()
        ),
    )
    .unwrap_or_else(|error| panic!("Worker config must be written: {error}"));
    let uid = Uid::effective();
    let gid = Gid::effective();
    (
        StorageBootstrap {
            config,
            runtime_directory: runtime_parent.join("agent-knowledge"),
            worker_owner: OsString::from(uid.as_raw().to_string()),
            worker_group: OsString::from(gid.as_raw().to_string()),
            queue_owner: OsString::from(uid.as_raw().to_string()),
            queue_group: OsString::from(gid.as_raw().to_string()),
            gateway_group: OsString::from(gid.as_raw().to_string()),
            ingress_group: OsString::from(gid.as_raw().to_string()),
        },
        StorageIdentities {
            administrative_owner: uid,
            administrative_group: gid,
            worker_owner: uid,
            worker_group: gid,
            queue_owner: uid,
            queue_group: gid,
            gateway_group: gid,
            ingress_group: gid,
        },
    )
}

#[test]
fn initializes_fresh_storage_and_is_idempotent() {
    let root = TestDirectory::new();
    let (request, identities) = fixture(root.path());
    let mut first = Vec::new();
    bootstrap_storage_with_ids(&request, identities, &mut first)
        .unwrap_or_else(|error| panic!("fresh bootstrap must succeed: {error}"));
    assert_eq!(first, b"{\"status\":\"initialized\"}\n");

    let storage = root.path().join("storage");
    let queue = fs::symlink_metadata(storage.join("queue"))
        .unwrap_or_else(|error| panic!("queue metadata must exist: {error}"));
    assert_eq!(queue.permissions().mode() & 0o7777, 0o2770);
    assert_eq!(queue.uid(), identities.queue_owner.as_raw());
    assert_eq!(queue.gid(), identities.queue_group.as_raw());
    assert!(storage.join("content/.git").is_file());
    assert!(storage.join("repository/refs/heads/main").is_file());
    assert!(storage.join("releases/by-id").is_dir());

    let mut second = Vec::new();
    bootstrap_storage_with_ids(&request, identities, &mut second)
        .unwrap_or_else(|error| panic!("completed bootstrap must be idempotent: {error}"));
    assert_eq!(second, b"{\"status\":\"already_initialized\"}\n");

    fs::remove_dir(&request.runtime_directory)
        .unwrap_or_else(|error| panic!("empty runtime directory must be removable: {error}"));
    let mut after_restart = Vec::new();
    bootstrap_storage_with_ids(&request, identities, &mut after_restart)
        .unwrap_or_else(|error| panic!("ephemeral runtime must be recreated: {error}"));
    assert_eq!(after_restart, b"{\"status\":\"already_initialized\"}\n");
    assert!(request.runtime_directory.is_dir());
}

#[test]
fn refuses_nonempty_unmarked_storage() {
    let root = TestDirectory::new();
    let (request, identities) = fixture(root.path());
    let queue = root.path().join("storage/queue");
    fs::create_dir_all(&queue)
        .unwrap_or_else(|error| panic!("partial queue must be created: {error}"));
    fs::write(queue.join("unexpected"), b"partial")
        .unwrap_or_else(|error| panic!("partial state must be written: {error}"));

    assert!(matches!(
        bootstrap_storage_with_ids(&request, identities, Vec::new()),
        Err(StorageBootstrapError::PartialInitialization(path)) if path == queue
    ));
}

#[test]
fn rejects_an_invalid_branch_before_creating_storage() {
    let root = TestDirectory::new();
    let (request, identities) = fixture(root.path());
    let config = fs::read_to_string(&request.config)
        .unwrap_or_else(|error| panic!("Worker config must be readable: {error}"));
    fs::write(
        &request.config,
        config.replace("official_branch: main", "official_branch: bad..branch"),
    )
    .unwrap_or_else(|error| panic!("invalid branch fixture must be written: {error}"));

    assert!(matches!(
        bootstrap_storage_with_ids(&request, identities, Vec::new()),
        Err(StorageBootstrapError::GitFailed(_))
    ));
    assert!(!root.path().join("storage").exists());
}

#[test]
fn refuses_a_marker_for_different_identities() {
    let root = TestDirectory::new();
    let (request, identities) = fixture(root.path());
    bootstrap_storage_with_ids(&request, identities, Vec::new())
        .unwrap_or_else(|error| panic!("fresh bootstrap must succeed: {error}"));
    let different = StorageIdentities {
        gateway_group: Gid::from_raw(identities.gateway_group.as_raw().saturating_add(1)),
        ..identities
    };

    assert!(matches!(
        bootstrap_storage_with_ids(&request, different, Vec::new()),
        Err(StorageBootstrapError::MarkerMismatch)
    ));
}

#[test]
fn accepts_an_option_looking_official_branch() {
    for branch in ["--detach", "HEAD"] {
        let root = TestDirectory::new();
        let (request, identities) = fixture(root.path());
        let config = fs::read_to_string(&request.config)
            .unwrap_or_else(|error| panic!("Worker config must be readable: {error}"));
        fs::write(
            &request.config,
            config.replace(
                "official_branch: main",
                &format!("official_branch: {branch}"),
            ),
        )
        .unwrap_or_else(|error| panic!("branch fixture must be written: {error}"));

        bootstrap_storage_with_ids(&request, identities, Vec::new()).unwrap_or_else(|error| {
            panic!("ambiguous-looking branch {branch} must initialize safely: {error}")
        });
        assert!(
            root.path()
                .join(format!("storage/repository/refs/heads/{branch}"))
                .is_file()
        );
    }
}

#[test]
fn runtime_path_is_not_part_of_the_durable_marker() {
    let root = TestDirectory::new();
    let (mut request, identities) = fixture(root.path());
    bootstrap_storage_with_ids(&request, identities, Vec::new())
        .unwrap_or_else(|error| panic!("fresh bootstrap must succeed: {error}"));
    fs::remove_dir(&request.runtime_directory)
        .unwrap_or_else(|error| panic!("old runtime directory must be removable: {error}"));
    request.runtime_directory = root.path().join("run/reconfigured");

    let mut output = Vec::new();
    bootstrap_storage_with_ids(&request, identities, &mut output)
        .unwrap_or_else(|error| panic!("new ephemeral runtime must be accepted: {error}"));
    assert_eq!(output, b"{\"status\":\"already_initialized\"}\n");
    assert!(request.runtime_directory.is_dir());
}

#[test]
fn rejects_a_runtime_path_inside_the_durable_parent() {
    let root = TestDirectory::new();
    let (mut request, identities) = fixture(root.path());
    request.runtime_directory = root.path().join("storage/runtime");

    assert!(matches!(
        bootstrap_storage_with_ids(&request, identities, Vec::new()),
        Err(StorageBootstrapError::InvalidRuntimeDirectory)
    ));
    assert!(!root.path().join("storage").exists());
}

#[test]
fn rejects_an_unknown_unmarked_storage_sibling() {
    let root = TestDirectory::new();
    let (request, identities) = fixture(root.path());
    let storage = root.path().join("storage");
    fs::create_dir(&storage)
        .unwrap_or_else(|error| panic!("storage root must be created: {error}"));
    fs::write(storage.join("unexpected"), b"partial")
        .unwrap_or_else(|error| panic!("unexpected sibling must be written: {error}"));

    assert!(matches!(
        bootstrap_storage_with_ids(&request, identities, Vec::new()),
        Err(StorageBootstrapError::PartialInitialization(path)) if path == storage
    ));
}

#[test]
fn marked_storage_rejects_a_corrupt_repository_binding() {
    let root = TestDirectory::new();
    let (request, identities) = fixture(root.path());
    bootstrap_storage_with_ids(&request, identities, Vec::new())
        .unwrap_or_else(|error| panic!("fresh bootstrap must succeed: {error}"));
    fs::write(
        root.path()
            .join("storage/work/.agent-knowledge-repository-binding-v2"),
        b"corrupt",
    )
    .unwrap_or_else(|error| panic!("binding must be replaceable in the fixture: {error}"));

    assert!(matches!(
        bootstrap_storage_with_ids(&request, identities, Vec::new()),
        Err(StorageBootstrapError::Component(
            "repository binding validation",
            _
        ))
    ));
}

#[test]
fn marked_storage_rejects_descendant_permission_drift() {
    let root = TestDirectory::new();
    let (request, identities) = fixture(root.path());
    bootstrap_storage_with_ids(&request, identities, Vec::new())
        .unwrap_or_else(|error| panic!("fresh bootstrap must succeed: {error}"));
    let head = root.path().join("storage/repository/HEAD");
    fs::set_permissions(&head, fs::Permissions::from_mode(0o600))
        .unwrap_or_else(|error| panic!("fixture permissions must change: {error}"));

    assert!(matches!(
        bootstrap_storage_with_ids(&request, identities, Vec::new()),
        Err(StorageBootstrapError::Permissions(path, _))
            if path == root.path().join("storage/repository")
    ));
}

#[test]
fn marked_storage_accepts_worker_queue_metadata() {
    let root = TestDirectory::new();
    let (request, identities) = fixture(root.path());
    bootstrap_storage_with_ids(&request, identities, Vec::new())
        .unwrap_or_else(|error| panic!("fresh bootstrap must succeed: {error}"));
    let worker_temporary = root.path().join("storage/queue/worker-tmp");
    fs::set_permissions(&worker_temporary, fs::Permissions::from_mode(0o2770))
        .unwrap_or_else(|error| panic!("Worker temporary directory mode must be set: {error}"));
    let temporary = worker_temporary.join(".phase-fictional");
    fs::write(&temporary, b"fictional phase")
        .unwrap_or_else(|error| panic!("Worker metadata fixture must be written: {error}"));
    fs::set_permissions(&temporary, fs::Permissions::from_mode(0o640))
        .unwrap_or_else(|error| panic!("Worker metadata mode must be set: {error}"));

    let mut output = Vec::new();
    bootstrap_storage_with_ids(&request, identities, &mut output)
        .unwrap_or_else(|error| panic!("valid Worker metadata must be accepted: {error}"));
    assert_eq!(output, b"{\"status\":\"already_initialized\"}\n");
}

#[test]
fn marker_covers_the_initial_commit_identity() {
    let root = TestDirectory::new();
    let (request, identities) = fixture(root.path());
    bootstrap_storage_with_ids(&request, identities, Vec::new())
        .unwrap_or_else(|error| panic!("fresh bootstrap must succeed: {error}"));
    let config = fs::read_to_string(&request.config)
        .unwrap_or_else(|error| panic!("Worker config must be readable: {error}"));
    fs::write(
        &request.config,
        config.replace(
            "author_email: worker@example.invalid",
            "author_email: replacement@example.invalid",
        ),
    )
    .unwrap_or_else(|error| panic!("identity fixture must be written: {error}"));

    assert!(matches!(
        bootstrap_storage_with_ids(&request, identities, Vec::new()),
        Err(StorageBootstrapError::MarkerMismatch)
    ));
}

use std::ffi::OsString;
use std::fs::{self, File};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use nix::unistd::{Gid, Uid};
use ulid::Ulid;
use xattr::FileExt as _;

use super::{
    MARKER_NAME, ServiceMemberships, StorageBootstrap, StorageBootstrapError, StorageIdentities,
    StorageMigrationError, bootstrap_administrative_group, bootstrap_storage_with_ids,
    bootstrap_storage_with_ids_and_git_check, validate_service_identities,
    validate_service_memberships,
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
            gateway_owner: OsString::from(uid.as_raw().to_string()),
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
            gateway_owner: uid,
            gateway_group: gid,
            ingress_group: gid,
        },
    )
}

fn separated_service_identities() -> StorageIdentities {
    StorageIdentities {
        administrative_owner: Uid::from_raw(0),
        administrative_group: Gid::from_raw(0),
        worker_owner: Uid::from_raw(10_003),
        worker_group: Gid::from_raw(10_003),
        queue_owner: Uid::from_raw(10_002),
        queue_group: Gid::from_raw(10_002),
        gateway_owner: Uid::from_raw(10_001),
        gateway_group: Gid::from_raw(10_001),
        ingress_group: Gid::from_raw(10_004),
    }
}

#[test]
fn requires_non_root_distinct_service_identities() {
    assert_eq!(bootstrap_administrative_group(), Gid::from_raw(0));
    let identities = separated_service_identities();
    validate_service_identities(identities)
        .unwrap_or_else(|error| panic!("separated service identities must be valid: {error}"));

    let mut root_uid = identities;
    root_uid.worker_owner = Uid::from_raw(0);
    let mut shared_uid = identities;
    shared_uid.queue_owner = shared_uid.worker_owner;
    let mut root_group = identities;
    root_group.gateway_group = Gid::from_raw(0);
    let mut shared_gateway_uid = identities;
    shared_gateway_uid.gateway_owner = shared_gateway_uid.queue_owner;
    let mut shared_group = identities;
    shared_group.ingress_group = shared_group.queue_group;

    for invalid in [
        root_uid,
        shared_uid,
        root_group,
        shared_gateway_uid,
        shared_group,
    ] {
        assert!(matches!(
            validate_service_identities(invalid),
            Err(StorageBootstrapError::UnsafeServiceIdentities)
        ));
    }
}

#[test]
fn requires_the_exact_service_role_membership_matrix() {
    let identities = separated_service_identities();
    let memberships = ServiceMemberships {
        worker_primary: identities.worker_group,
        worker_groups: vec![identities.worker_group, identities.queue_group],
        queue_primary: identities.queue_group,
        queue_groups: vec![identities.queue_group],
        gateway_primary: identities.gateway_group,
        gateway_groups: vec![identities.gateway_group, identities.ingress_group],
    };
    validate_service_memberships(identities, &memberships)
        .unwrap_or_else(|error| panic!("the intended membership matrix must be valid: {error}"));

    let mut gateway_can_write_queue = ServiceMemberships {
        gateway_groups: vec![
            identities.gateway_group,
            identities.ingress_group,
            identities.queue_group,
        ],
        ..memberships
    };
    assert!(matches!(
        validate_service_memberships(identities, &gateway_can_write_queue),
        Err(StorageBootstrapError::UnsafeServiceMemberships)
    ));

    gateway_can_write_queue.gateway_groups =
        vec![identities.gateway_group, identities.ingress_group];
    gateway_can_write_queue.worker_groups = vec![identities.worker_group];
    assert!(matches!(
        validate_service_memberships(identities, &gateway_can_write_queue),
        Err(StorageBootstrapError::UnsafeServiceMemberships)
    ));
}

fn set_extended_posix_acl(path: &Path, name: &str) {
    // Linux POSIX ACL xattrs use a version header followed by tag, permission,
    // and ID entries. A named user entry plus the mask keeps the ACL extended
    // instead of allowing the kernel to reduce it to mode bits.
    let named_uid = Uid::effective().as_raw();
    let mut value = 2_u32.to_le_bytes().to_vec();
    for (tag, permissions, id) in [
        (0x01_u16, 0o7_u16, u32::MAX),
        (0x02_u16, 0o7_u16, named_uid),
        (0x04_u16, 0o5_u16, u32::MAX),
        (0x10_u16, 0o7_u16, u32::MAX),
        (0x20_u16, 0o5_u16, u32::MAX),
    ] {
        value.extend_from_slice(&tag.to_le_bytes());
        value.extend_from_slice(&permissions.to_le_bytes());
        value.extend_from_slice(&id.to_le_bytes());
    }
    let file =
        File::open(path).unwrap_or_else(|error| panic!("ACL fixture must be opened: {error}"));
    file.set_xattr(name, &value)
        .unwrap_or_else(|error| panic!("ACL fixture must be written: {error}"));
    assert!(
        file.get_xattr(name)
            .unwrap_or_else(|error| panic!("ACL fixture must be readable: {error}"))
            .is_some()
    );
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
    let marker = fs::symlink_metadata(storage.join(MARKER_NAME))
        .unwrap_or_else(|error| panic!("bootstrap marker metadata must exist: {error}"));
    assert_eq!(marker.uid(), identities.administrative_owner.as_raw());
    assert_eq!(marker.gid(), identities.administrative_group.as_raw());
    assert_eq!(marker.permissions().mode() & 0o7777, 0o444);

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
fn rejects_inherited_posix_acl_before_mutating_fresh_storage() {
    let root = TestDirectory::new();
    let (request, identities) = fixture(root.path());
    let storage = root.path().join("storage");
    fs::create_dir(&storage)
        .unwrap_or_else(|error| panic!("storage fixture must be created: {error}"));
    set_extended_posix_acl(&storage, "system.posix_acl_default");
    let mode_before = fs::symlink_metadata(&storage)
        .unwrap_or_else(|error| panic!("storage fixture metadata must be readable: {error}"))
        .permissions()
        .mode();

    assert!(matches!(
        bootstrap_storage_with_ids(&request, identities, Vec::new()),
        Err(StorageBootstrapError::Permissions(path, StorageMigrationError::PosixAcl(acl_path)))
            if path == storage && acl_path.as_os_str().is_empty()
    ));
    assert_eq!(
        fs::symlink_metadata(&storage)
            .unwrap_or_else(|error| panic!(
                "storage fixture metadata must remain readable: {error}"
            ))
            .permissions()
            .mode(),
        mode_before
    );
    assert!(
        fs::read_dir(&storage)
            .unwrap_or_else(|error| panic!("storage fixture must remain readable: {error}"))
            .next()
            .is_none()
    );
}

#[test]
fn rejects_storage_parent_default_acl_before_creating_fresh_storage() {
    let root = TestDirectory::new();
    let (request, identities) = fixture(root.path());
    set_extended_posix_acl(root.path(), "system.posix_acl_default");

    assert!(matches!(
        bootstrap_storage_with_ids(&request, identities, Vec::new()),
        Err(StorageBootstrapError::Permissions(path, StorageMigrationError::PosixAcl(acl_path)))
            if path == root.path() && acl_path.as_os_str().is_empty()
    ));
    assert!(!root.path().join("storage").exists());
}

#[test]
fn rejects_unsupported_git_before_creating_fresh_storage() {
    let root = TestDirectory::new();
    let (request, identities) = fixture(root.path());

    assert!(matches!(
        bootstrap_storage_with_ids_and_git_check(&request, identities, Vec::new(), || {
            Err(Box::new(
                agent_knowledge_repository::GitTransactionError::UnsupportedGitVersion {
                    found: "git version 2.35.8".into(),
                },
            ))
        }),
        Err(StorageBootstrapError::Component(
            "Git compatibility validation",
            _
        ))
    ));
    assert!(!root.path().join("storage").exists());
}

#[test]
fn rejects_nonempty_runtime_before_creating_fresh_storage() {
    let root = TestDirectory::new();
    let (request, identities) = fixture(root.path());
    fs::create_dir(&request.runtime_directory)
        .unwrap_or_else(|error| panic!("runtime fixture must be created: {error}"));
    fs::write(
        request.runtime_directory.join("fictional-request.json"),
        b"{}",
    )
    .unwrap_or_else(|error| panic!("runtime fixture must be populated: {error}"));

    assert!(matches!(
        bootstrap_storage_with_ids(&request, identities, Vec::new()),
        Err(StorageBootstrapError::PartialInitialization(path))
            if path == request.runtime_directory
    ));
    assert!(!root.path().join("storage").exists());
}

#[test]
fn rejects_runtime_acl_before_creating_fresh_storage() {
    let root = TestDirectory::new();
    let (request, identities) = fixture(root.path());
    fs::create_dir(&request.runtime_directory)
        .unwrap_or_else(|error| panic!("runtime fixture must be created: {error}"));
    set_extended_posix_acl(&request.runtime_directory, "system.posix_acl_access");

    assert!(matches!(
        bootstrap_storage_with_ids(&request, identities, Vec::new()),
        Err(StorageBootstrapError::Permissions(path, StorageMigrationError::PosixAcl(acl_path)))
            if path == request.runtime_directory && acl_path.as_os_str().is_empty()
    ));
    assert!(!root.path().join("storage").exists());
}

#[test]
fn rejects_runtime_parent_default_acl_before_creating_fresh_storage() {
    let root = TestDirectory::new();
    let (request, identities) = fixture(root.path());
    let runtime_parent = request
        .runtime_directory
        .parent()
        .unwrap_or_else(|| panic!("runtime fixture must have a parent"));
    set_extended_posix_acl(runtime_parent, "system.posix_acl_default");

    assert!(matches!(
        bootstrap_storage_with_ids(&request, identities, Vec::new()),
        Err(StorageBootstrapError::Permissions(path, StorageMigrationError::PosixAcl(acl_path)))
            if path == runtime_parent && acl_path.as_os_str().is_empty()
    ));
    assert!(!request.runtime_directory.exists());
    assert!(!root.path().join("storage").exists());
}

#[test]
fn rejects_posix_acl_added_to_ephemeral_runtime() {
    let root = TestDirectory::new();
    let (request, identities) = fixture(root.path());
    bootstrap_storage_with_ids(&request, identities, Vec::new())
        .unwrap_or_else(|error| panic!("fresh bootstrap must succeed: {error}"));
    set_extended_posix_acl(&request.runtime_directory, "system.posix_acl_access");

    assert!(matches!(
        bootstrap_storage_with_ids(&request, identities, Vec::new()),
        Err(StorageBootstrapError::Permissions(path, StorageMigrationError::PosixAcl(acl_path)))
            if path == request.runtime_directory && acl_path.as_os_str().is_empty()
    ));
}

#[test]
fn rejects_posix_acl_added_to_initialized_storage_file() {
    let root = TestDirectory::new();
    let (request, identities) = fixture(root.path());
    bootstrap_storage_with_ids(&request, identities, Vec::new())
        .unwrap_or_else(|error| panic!("fresh bootstrap must succeed: {error}"));
    let repository = root.path().join("storage/repository");
    set_extended_posix_acl(&repository.join("HEAD"), "system.posix_acl_access");

    assert!(matches!(
        bootstrap_storage_with_ids(&request, identities, Vec::new()),
        Err(StorageBootstrapError::Permissions(path, StorageMigrationError::PosixAcl(acl_path)))
            if path == repository && acl_path == Path::new("HEAD")
    ));
}

#[test]
fn refuses_nonempty_unmarked_storage() {
    let root = TestDirectory::new();
    let (request, identities) = fixture(root.path());
    let queue = root.path().join("storage/queue");
    fs::create_dir_all(&queue)
        .unwrap_or_else(|error| panic!("partial queue must be created: {error}"));
    fs::set_permissions(
        root.path().join("storage"),
        fs::Permissions::from_mode(0o755),
    )
    .unwrap_or_else(|error| panic!("storage root mode must be set: {error}"));
    fs::set_permissions(&queue, fs::Permissions::from_mode(0o700))
        .unwrap_or_else(|error| panic!("partial queue mode must be set: {error}"));
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
    fs::set_permissions(&storage, fs::Permissions::from_mode(0o755))
        .unwrap_or_else(|error| panic!("storage root mode must be set: {error}"));
    fs::write(storage.join("unexpected"), b"partial")
        .unwrap_or_else(|error| panic!("unexpected sibling must be written: {error}"));

    assert!(matches!(
        bootstrap_storage_with_ids(&request, identities, Vec::new()),
        Err(StorageBootstrapError::PartialInitialization(path)) if path == storage
    ));
    assert_eq!(
        fs::metadata(&storage)
            .unwrap_or_else(|error| panic!("rejected root metadata must be readable: {error}"))
            .permissions()
            .mode()
            & 0o7777,
        0o755
    );
}

#[test]
fn rejects_writable_unmarked_children_without_mutating_them() {
    let root = TestDirectory::new();
    let (request, identities) = fixture(root.path());
    let storage = root.path().join("storage");
    let queue = storage.join("queue");
    fs::create_dir(&storage)
        .unwrap_or_else(|error| panic!("storage root must be created: {error}"));
    fs::set_permissions(&storage, fs::Permissions::from_mode(0o755))
        .unwrap_or_else(|error| panic!("storage root mode must be set: {error}"));
    fs::create_dir(&queue).unwrap_or_else(|error| panic!("queue root must be created: {error}"));
    fs::set_permissions(&queue, fs::Permissions::from_mode(0o777))
        .unwrap_or_else(|error| panic!("queue root mode must be set: {error}"));

    assert!(matches!(
        bootstrap_storage_with_ids(&request, identities, Vec::new()),
        Err(StorageBootstrapError::UnsafePath(path)) if path == queue
    ));
    assert_eq!(
        fs::metadata(&queue)
            .unwrap_or_else(|error| panic!("rejected queue metadata must be readable: {error}"))
            .permissions()
            .mode()
            & 0o7777,
        0o777
    );
}

#[test]
fn accepts_an_empty_filesystem_lost_and_found_directory() {
    let root = TestDirectory::new();
    let (request, identities) = fixture(root.path());
    let storage = root.path().join("storage");
    let lost_and_found = storage.join("lost+found");
    fs::create_dir(&storage)
        .unwrap_or_else(|error| panic!("storage root must be created: {error}"));
    fs::set_permissions(&storage, fs::Permissions::from_mode(0o755))
        .unwrap_or_else(|error| panic!("storage root mode must be set: {error}"));
    fs::create_dir(&lost_and_found)
        .unwrap_or_else(|error| panic!("lost+found fixture must be created: {error}"));
    fs::set_permissions(&lost_and_found, fs::Permissions::from_mode(0o700))
        .unwrap_or_else(|error| panic!("lost+found fixture mode must be set: {error}"));

    bootstrap_storage_with_ids(&request, identities, Vec::new())
        .unwrap_or_else(|error| panic!("empty lost+found must be accepted: {error}"));
    assert_eq!(
        fs::metadata(&lost_and_found)
            .unwrap_or_else(|error| panic!("lost+found metadata must be readable: {error}"))
            .permissions()
            .mode()
            & 0o7777,
        0o700
    );
}

#[test]
fn rejects_a_populated_filesystem_lost_and_found_directory() {
    let root = TestDirectory::new();
    let (request, identities) = fixture(root.path());
    let storage = root.path().join("storage");
    let lost_and_found = storage.join("lost+found");
    fs::create_dir(&storage)
        .unwrap_or_else(|error| panic!("storage root must be created: {error}"));
    fs::set_permissions(&storage, fs::Permissions::from_mode(0o755))
        .unwrap_or_else(|error| panic!("storage root mode must be set: {error}"));
    fs::create_dir(&lost_and_found)
        .unwrap_or_else(|error| panic!("lost+found fixture must be created: {error}"));
    fs::set_permissions(&lost_and_found, fs::Permissions::from_mode(0o700))
        .unwrap_or_else(|error| panic!("lost+found fixture mode must be set: {error}"));
    fs::write(lost_and_found.join("recovered-fictional"), b"fictional")
        .unwrap_or_else(|error| panic!("recovered fixture must be written: {error}"));

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
    fs::remove_dir(&request.runtime_directory)
        .unwrap_or_else(|error| panic!("runtime fixture must be removed: {error}"));

    assert!(matches!(
        bootstrap_storage_with_ids(&request, identities, Vec::new()),
        Err(StorageBootstrapError::Component(
            "repository binding validation",
            _
        ))
    ));
    assert!(!request.runtime_directory.exists());
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
    let temporary = worker_temporary.join(".phase-01K00000000000000000000000");
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

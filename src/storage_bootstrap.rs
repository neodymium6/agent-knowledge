use std::ffi::OsString;
use std::fmt;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Read, Write};
use std::os::unix::fs::DirBuilderExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use agent_knowledge_core::{PathAttestation, PathAttestationError};
use agent_knowledge_queue::{FileQueue, PackagePolicy, QueueReader};
use agent_knowledge_release::{ReleasePolicy, ReleaseReader, ReleaseStore};
use agent_knowledge_repository::{
    CommittedStore, GitRepository, GitTransactionError, trusted_git_program,
    validate_git_compatibility,
};
use agent_knowledge_worker::{WorkerConfigError, WorkerSettings};
use nix::sys::stat::Mode;
use nix::unistd::{Gid, Uid, fchown};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::admin::{
    StorageMigrationError, normalize_storage_directory, normalize_storage_tree, resolve_group,
    resolve_user, validate_bootstrap_source_tree, validate_queue_tree, validate_release_tree,
    validate_repository_tree, validate_same_storage_mount, validate_storage_directory_no_posix_acl,
    validate_storage_file_mount, validate_storage_tree,
};

const MARKER_NAME: &str = ".agent-knowledge-bootstrap-v1.json";
const MARKER_VERSION: u16 = 1;
const MAXIMUM_MARKER_BYTES: u64 = 64 * 1024;
const MAXIMUM_GIT_OUTPUT_BYTES: usize = 64 * 1024;
#[cfg(not(test))]
const STORAGE_BOOTSTRAP_UMASK: u32 = 0o077;

#[derive(Debug)]
pub(crate) struct StorageBootstrap {
    pub(crate) config: PathBuf,
    pub(crate) runtime_directory: PathBuf,
    pub(crate) worker_owner: OsString,
    pub(crate) worker_group: OsString,
    pub(crate) queue_owner: OsString,
    pub(crate) queue_group: OsString,
    pub(crate) gateway_group: OsString,
    pub(crate) ingress_group: OsString,
}

#[derive(Clone, Copy)]
struct StorageIdentities {
    administrative_owner: Uid,
    administrative_group: Gid,
    worker_owner: Uid,
    worker_group: Gid,
    queue_owner: Uid,
    queue_group: Gid,
    gateway_group: Gid,
    ingress_group: Gid,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct BootstrapMarker {
    schema_version: u16,
    queue_root: PathBuf,
    repository_root: PathBuf,
    content_root: PathBuf,
    work_root: PathBuf,
    release_root: PathBuf,
    official_branch: String,
    git_author_name: String,
    git_author_email: String,
    worker_uid: u32,
    worker_gid: u32,
    queue_uid: u32,
    queue_gid: u32,
    gateway_gid: u32,
    ingress_gid: u32,
}

#[derive(Serialize)]
struct BootstrapOutput<'a> {
    status: &'a str,
}

pub(crate) fn bootstrap_storage(
    request: &StorageBootstrap,
    output: impl Write,
) -> Result<(), StorageBootstrapError> {
    if !Uid::effective().is_root() {
        return Err(StorageBootstrapError::RootRequired);
    }
    #[cfg(not(test))]
    nix::sys::stat::umask(Mode::from_bits_truncate(STORAGE_BOOTSTRAP_UMASK));
    let identities = StorageIdentities {
        administrative_owner: Uid::effective(),
        administrative_group: bootstrap_administrative_group(),
        worker_owner: resolve_user(&request.worker_owner)
            .map_err(StorageBootstrapError::Identity)?,
        worker_group: resolve_group(&request.worker_group)
            .map_err(StorageBootstrapError::Identity)?,
        queue_owner: resolve_user(&request.queue_owner).map_err(StorageBootstrapError::Identity)?,
        queue_group: resolve_group(&request.queue_group)
            .map_err(StorageBootstrapError::Identity)?,
        gateway_group: resolve_group(&request.gateway_group)
            .map_err(StorageBootstrapError::Identity)?,
        ingress_group: resolve_group(&request.ingress_group)
            .map_err(StorageBootstrapError::Identity)?,
    };
    validate_service_identities(identities)?;
    bootstrap_storage_with_ids(request, identities, output)
}

fn bootstrap_administrative_group() -> Gid {
    Gid::from_raw(0)
}

fn validate_service_identities(identities: StorageIdentities) -> Result<(), StorageBootstrapError> {
    let service_uids = [identities.worker_owner, identities.queue_owner];
    let role_groups = [
        identities.worker_group,
        identities.queue_group,
        identities.gateway_group,
        identities.ingress_group,
    ];
    let duplicate_group = role_groups
        .iter()
        .enumerate()
        .any(|(index, group)| role_groups[..index].contains(group));
    if service_uids.iter().any(|uid| uid.as_raw() == 0)
        || service_uids[0] == service_uids[1]
        || role_groups.iter().any(|group| group.as_raw() == 0)
        || duplicate_group
    {
        Err(StorageBootstrapError::UnsafeServiceIdentities)
    } else {
        Ok(())
    }
}

fn bootstrap_storage_with_ids(
    request: &StorageBootstrap,
    identities: StorageIdentities,
    output: impl Write,
) -> Result<(), StorageBootstrapError> {
    bootstrap_storage_with_ids_and_git_check(request, identities, output, || {
        validate_git_compatibility().map_err(Box::new)
    })
}

fn bootstrap_storage_with_ids_and_git_check(
    request: &StorageBootstrap,
    identities: StorageIdentities,
    mut output: impl Write,
    git_check: impl FnOnce() -> Result<(), Box<GitTransactionError>>,
) -> Result<(), StorageBootstrapError> {
    let settings = WorkerSettings::load(&request.config).map_err(StorageBootstrapError::Config)?;
    let storage_root = common_storage_root(&settings)?;
    validate_trusted_parent(&storage_root, identities.administrative_owner)?;
    validate_trusted_parent(&request.runtime_directory, identities.administrative_owner)?;
    validate_parent_no_posix_acl(&storage_root)?;
    validate_runtime_directory(&request.runtime_directory, &storage_root)?;
    let marker_path = storage_root.join(MARKER_NAME);
    let expected_marker = BootstrapMarker::new(&settings, identities);
    validate_official_branch(settings.official_branch())?;
    git_check().map_err(|error| {
        StorageBootstrapError::Component("Git compatibility validation", error.to_string())
    })?;
    preflight_runtime_directory(&request.runtime_directory)?;
    ensure_directory(&storage_root)?;
    let storage_lock = lock_storage_root(&storage_root)?;
    revalidate_storage_lock(&storage_root, &storage_lock)?;
    validate_storage_directory_no_posix_acl(&storage_root)
        .map_err(|error| StorageBootstrapError::Permissions(storage_root.clone(), error))?;

    if path_exists(&marker_path)? {
        require_directory_metadata(
            &storage_root,
            identities.administrative_owner,
            identities.queue_group,
            0o751,
        )?;
        validate_storage_file_mount(&storage_root, &marker_path)
            .map_err(|_| StorageBootstrapError::InvalidMarker)?;
        let marker = read_marker(
            &marker_path,
            identities.administrative_owner,
            identities.administrative_group,
        )?;
        if marker != expected_marker {
            return Err(StorageBootstrapError::MarkerMismatch);
        }
        validate_same_storage_mount(&storage_root, &storage_paths(&settings))
            .map_err(|error| StorageBootstrapError::Permissions(storage_root.clone(), error))?;
        validate_durable_initialized(&settings, identities)?;
        initialize_runtime_directory(&request.runtime_directory, identities)?;
        revalidate_storage_lock(&storage_root, &storage_lock)?;
        validate_initialized(&settings, &request.runtime_directory, identities)?;
        drop(storage_lock);
        return write_output(&mut output, "already_initialized");
    }

    validate_bootstrap_source_tree(&storage_root)
        .map_err(|error| StorageBootstrapError::Permissions(storage_root.clone(), error))?;
    validate_unmarked_storage_root(&storage_root, &settings, identities)?;
    for path in storage_paths(&settings) {
        require_safe_empty_or_absent(path, identities.administrative_owner)?;
    }
    for path in storage_paths(&settings) {
        ensure_directory(path)?;
    }
    revalidate_storage_lock(&storage_root, &storage_lock)?;
    validate_same_storage_mount(&storage_root, &storage_paths(&settings))
        .map_err(|error| StorageBootstrapError::Permissions(storage_root.clone(), error))?;
    normalize_storage_directory(
        &storage_root,
        identities.administrative_owner,
        identities.queue_group,
        Mode::from_bits_truncate(0o751),
    )
    .map_err(|error| StorageBootstrapError::Permissions(storage_root.clone(), error))?;
    revalidate_storage_lock(&storage_root, &storage_lock)?;
    for path in storage_paths(&settings) {
        normalize_storage_directory(
            path,
            identities.administrative_owner,
            identities.administrative_group,
            Mode::from_bits_truncate(0o700),
        )
        .map_err(|error| StorageBootstrapError::Permissions(path.into(), error))?;
    }
    initialize_runtime_directory(&request.runtime_directory, identities)?;

    FileQueue::initialize(settings.queue_root(), PackagePolicy::default()).map_err(|error| {
        StorageBootstrapError::Component("queue initialization", error.to_string())
    })?;
    revalidate_storage_lock(&storage_root, &storage_lock)?;
    initialize_repository(&settings)?;
    GitRepository::open(
        settings.repository_root(),
        settings.content_root(),
        settings.work_root(),
        settings.official_branch(),
        settings.identity().clone(),
    )
    .map_err(|error| {
        StorageBootstrapError::Component("repository validation", error.to_string())
    })?;
    ReleaseStore::open(settings.release_root(), ReleasePolicy::default()).map_err(|error| {
        StorageBootstrapError::Component("release initialization", error.to_string())
    })?;
    revalidate_storage_lock(&storage_root, &storage_lock)?;

    normalize_storage_tree(
        settings.queue_root(),
        identities.queue_owner,
        identities.queue_group,
        Mode::from_bits_truncate(0o2770),
        Mode::from_bits_truncate(0o660),
    )
    .map_err(|error| StorageBootstrapError::Permissions(settings.queue_root().into(), error))?;
    for path in [settings.repository_root(), settings.content_root()] {
        normalize_storage_tree(
            path,
            identities.worker_owner,
            identities.gateway_group,
            Mode::from_bits_truncate(0o2750),
            Mode::from_bits_truncate(0o640),
        )
        .map_err(|error| StorageBootstrapError::Permissions(path.into(), error))?;
    }
    for path in [settings.work_root(), settings.release_root()] {
        normalize_storage_tree(
            path,
            identities.worker_owner,
            identities.worker_group,
            Mode::from_bits_truncate(0o750),
            Mode::from_bits_truncate(0o640),
        )
        .map_err(|error| StorageBootstrapError::Permissions(path.into(), error))?;
    }
    revalidate_storage_lock(&storage_root, &storage_lock)?;
    validate_initialized(&settings, &request.runtime_directory, identities)?;
    sync_storage_filesystem(&storage_root)?;
    revalidate_storage_lock(&storage_root, &storage_lock)?;
    write_marker(
        &marker_path,
        &expected_marker,
        identities.administrative_owner,
        identities.administrative_group,
    )?;
    drop(storage_lock);
    write_output(&mut output, "initialized")
}

fn validate_official_branch(branch: &str) -> Result<(), StorageBootstrapError> {
    let reference = format!("refs/heads/{branch}");
    run_git(
        [
            OsString::from("check-ref-format"),
            OsString::from(reference),
        ],
        &[],
    )
    .map(|_| ())
}

fn initialize_runtime_directory(
    runtime_directory: &Path,
    identities: StorageIdentities,
) -> Result<(), StorageBootstrapError> {
    require_absent_or_empty(runtime_directory)?;
    ensure_directory(runtime_directory)?;
    normalize_storage_tree(
        runtime_directory,
        identities.queue_owner,
        identities.ingress_group,
        Mode::from_bits_truncate(0o2750),
        Mode::from_bits_truncate(0o640),
    )
    .map_err(|error| StorageBootstrapError::Permissions(runtime_directory.into(), error))
}

fn preflight_runtime_directory(runtime_directory: &Path) -> Result<(), StorageBootstrapError> {
    validate_parent_no_posix_acl(runtime_directory)?;
    require_absent_or_empty(runtime_directory)?;
    if path_exists(runtime_directory)? {
        validate_bootstrap_source_tree(runtime_directory).map_err(|error| {
            StorageBootstrapError::Permissions(runtime_directory.to_path_buf(), error)
        })?;
    }
    Ok(())
}

fn validate_parent_no_posix_acl(path: &Path) -> Result<(), StorageBootstrapError> {
    let parent = path.parent().ok_or(StorageBootstrapError::StorageLayout)?;
    validate_storage_directory_no_posix_acl(parent)
        .map_err(|error| StorageBootstrapError::Permissions(parent.to_path_buf(), error))
}

impl BootstrapMarker {
    fn new(settings: &WorkerSettings, ids: StorageIdentities) -> Self {
        Self {
            schema_version: MARKER_VERSION,
            queue_root: settings.queue_root().to_path_buf(),
            repository_root: settings.repository_root().to_path_buf(),
            content_root: settings.content_root().to_path_buf(),
            work_root: settings.work_root().to_path_buf(),
            release_root: settings.release_root().to_path_buf(),
            official_branch: settings.official_branch().to_owned(),
            git_author_name: settings.identity().name().to_owned(),
            git_author_email: settings.identity().email().to_owned(),
            worker_uid: ids.worker_owner.as_raw(),
            worker_gid: ids.worker_group.as_raw(),
            queue_uid: ids.queue_owner.as_raw(),
            queue_gid: ids.queue_group.as_raw(),
            gateway_gid: ids.gateway_group.as_raw(),
            ingress_gid: ids.ingress_group.as_raw(),
        }
    }
}

fn lock_storage_root(path: &Path) -> Result<File, StorageBootstrapError> {
    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC)
        .open(path)
        .map_err(StorageBootstrapError::Io)?;
    match directory.try_lock() {
        Ok(()) => Ok(directory),
        Err(TryLockError::WouldBlock) => Err(StorageBootstrapError::StorageBusy),
        Err(TryLockError::Error(error)) => Err(StorageBootstrapError::Io(error)),
    }
}

fn revalidate_storage_lock(path: &Path, locked: &File) -> Result<(), StorageBootstrapError> {
    let observed =
        PathAttestation::capture(path, locked).map_err(StorageBootstrapError::Attestation)?;
    if observed.path() == path {
        Ok(())
    } else {
        Err(StorageBootstrapError::UnsafePath(path.to_path_buf()))
    }
}

fn validate_trusted_parent(path: &Path, owner: Uid) -> Result<(), StorageBootstrapError> {
    if !owner.is_root() {
        return Ok(());
    }
    let parent = path
        .parent()
        .ok_or(StorageBootstrapError::UntrustedAncestry(path.to_path_buf()))?;
    let canonical = fs::canonicalize(parent).map_err(StorageBootstrapError::Io)?;
    if canonical != parent {
        return Err(StorageBootstrapError::UntrustedAncestry(path.to_path_buf()));
    }
    for ancestor in parent.ancestors() {
        let metadata = fs::symlink_metadata(ancestor).map_err(StorageBootstrapError::Io)?;
        if !metadata.file_type().is_dir()
            || metadata.uid() != owner.as_raw()
            || metadata.permissions().mode() & 0o022 != 0
        {
            return Err(StorageBootstrapError::UntrustedAncestry(path.to_path_buf()));
        }
    }
    Ok(())
}

fn validate_unmarked_storage_root(
    storage_root: &Path,
    settings: &WorkerSettings,
    identities: StorageIdentities,
) -> Result<(), StorageBootstrapError> {
    let metadata = fs::symlink_metadata(storage_root).map_err(StorageBootstrapError::Io)?;
    if !metadata.file_type().is_dir()
        || metadata.uid() != identities.administrative_owner.as_raw()
        || metadata.permissions().mode() & 0o022 != 0
    {
        return Err(StorageBootstrapError::UnsafePath(
            storage_root.to_path_buf(),
        ));
    }
    let expected = storage_paths(settings);
    let mut lost_and_found = None;
    for entry in fs::read_dir(storage_root).map_err(StorageBootstrapError::Io)? {
        let entry = entry.map_err(StorageBootstrapError::Io)?;
        let path = entry.path();
        if expected.iter().any(|expected_path| *expected_path == path) {
            continue;
        }
        if entry.file_name() == "lost+found"
            && valid_empty_lost_and_found(&path, identities.administrative_owner)?
        {
            lost_and_found = Some(path);
        } else {
            return Err(StorageBootstrapError::PartialInitialization(
                storage_root.to_path_buf(),
            ));
        }
    }
    if let Some(path) = lost_and_found {
        validate_same_storage_mount(storage_root, &[path.as_path()]).map_err(|error| {
            StorageBootstrapError::Permissions(storage_root.to_path_buf(), error)
        })?;
    }
    Ok(())
}

fn valid_empty_lost_and_found(path: &Path, owner: Uid) -> Result<bool, StorageBootstrapError> {
    let metadata = fs::symlink_metadata(path).map_err(StorageBootstrapError::Io)?;
    if !metadata.file_type().is_dir()
        || metadata.uid() != owner.as_raw()
        || metadata.permissions().mode() & 0o7777 != 0o700
    {
        return Ok(false);
    }
    let mut entries = fs::read_dir(path).map_err(StorageBootstrapError::Io)?;
    Ok(entries
        .next()
        .transpose()
        .map_err(StorageBootstrapError::Io)?
        .is_none())
}

fn storage_paths(settings: &WorkerSettings) -> [&Path; 5] {
    [
        settings.queue_root(),
        settings.repository_root(),
        settings.content_root(),
        settings.work_root(),
        settings.release_root(),
    ]
}

fn common_storage_root(settings: &WorkerSettings) -> Result<PathBuf, StorageBootstrapError> {
    let paths = storage_paths(settings);
    let Some(parent) = paths[0].parent() else {
        return Err(StorageBootstrapError::StorageLayout);
    };
    if parent.parent().is_none() || paths.iter().any(|path| path.parent() != Some(parent)) {
        return Err(StorageBootstrapError::StorageLayout);
    }
    Ok(parent.to_path_buf())
}

fn validate_runtime_directory(
    runtime_directory: &Path,
    storage_root: &Path,
) -> Result<(), StorageBootstrapError> {
    if !runtime_directory.is_absolute() || runtime_directory.parent().is_none() {
        return Err(StorageBootstrapError::InvalidRuntimeDirectory);
    }
    let runtime = PathAttestation::resolve_destination(runtime_directory)
        .map_err(StorageBootstrapError::Attestation)?;
    let durable = PathAttestation::resolve_destination(storage_root)
        .map_err(StorageBootstrapError::Attestation)?;
    if runtime.is_within(&durable) || durable.is_within(&runtime) {
        return Err(StorageBootstrapError::InvalidRuntimeDirectory);
    }
    Ok(())
}

fn require_absent_or_empty(path: &Path) -> Result<(), StorageBootstrapError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => {
            let mut entries = fs::read_dir(path).map_err(StorageBootstrapError::Io)?;
            if entries
                .next()
                .transpose()
                .map_err(StorageBootstrapError::Io)?
                .is_some()
            {
                Err(StorageBootstrapError::PartialInitialization(
                    path.to_path_buf(),
                ))
            } else {
                Ok(())
            }
        }
        Ok(_) => Err(StorageBootstrapError::UnsafePath(path.to_path_buf())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(StorageBootstrapError::Io(error)),
    }
}

fn require_safe_empty_or_absent(path: &Path, owner: Uid) -> Result<(), StorageBootstrapError> {
    match fs::symlink_metadata(path) {
        Ok(metadata)
            if metadata.file_type().is_dir()
                && metadata.uid() == owner.as_raw()
                && metadata.permissions().mode() & 0o022 == 0 =>
        {
            require_absent_or_empty(path)
        }
        Ok(_) => Err(StorageBootstrapError::UnsafePath(path.to_path_buf())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(StorageBootstrapError::Io(error)),
    }
}

fn ensure_directory(path: &Path) -> Result<(), StorageBootstrapError> {
    match fs::DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => sync_parent(path),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            let metadata = fs::symlink_metadata(path).map_err(StorageBootstrapError::Io)?;
            if metadata.file_type().is_dir() {
                Ok(())
            } else {
                Err(StorageBootstrapError::UnsafePath(path.to_path_buf()))
            }
        }
        Err(error) => Err(StorageBootstrapError::Io(error)),
    }
}

fn initialize_repository(settings: &WorkerSettings) -> Result<(), StorageBootstrapError> {
    let initial_ref = format!("refs/heads/{}", settings.official_branch());
    let initial_branch = format!("--initial-branch={}", settings.official_branch());
    run_git(
        [
            OsString::from("init"),
            OsString::from("--bare"),
            OsString::from("--template="),
            OsString::from(initial_branch),
            settings.repository_root().as_os_str().to_owned(),
        ],
        &[],
    )?;
    let tree = run_git(
        [
            git_directory_argument(settings.repository_root()),
            OsString::from("mktree"),
        ],
        &[],
    )?;
    let tree = parse_git_line(&tree)?;
    let name = format!("user.name={}", settings.identity().name());
    let email = format!("user.email={}", settings.identity().email());
    let commit = run_git(
        [
            git_directory_argument(settings.repository_root()),
            OsString::from("-c"),
            OsString::from(name),
            OsString::from("-c"),
            OsString::from(email),
            OsString::from("commit-tree"),
            OsString::from(tree),
        ],
        b"Initialize knowledge storage\n",
    )?;
    let commit = parse_git_line(&commit)?;
    run_git(
        [
            git_directory_argument(settings.repository_root()),
            OsString::from("update-ref"),
            OsString::from(&initial_ref),
            OsString::from(commit),
        ],
        &[],
    )?;
    run_git(
        [
            git_directory_argument(settings.repository_root()),
            OsString::from("symbolic-ref"),
            OsString::from("HEAD"),
            OsString::from(&initial_ref),
        ],
        &[],
    )?;
    run_git(
        [
            git_directory_argument(settings.repository_root()),
            OsString::from("worktree"),
            OsString::from("add"),
            OsString::from("--detach"),
            OsString::from("--"),
            settings.content_root().as_os_str().to_owned(),
            OsString::from(&initial_ref),
        ],
        &[],
    )?;
    run_git(
        [
            OsString::from("-C"),
            settings.content_root().as_os_str().to_owned(),
            OsString::from("symbolic-ref"),
            OsString::from("HEAD"),
            OsString::from(&initial_ref),
        ],
        &[],
    )?;
    Ok(())
}

fn git_directory_argument(path: &Path) -> OsString {
    let mut argument = OsString::from("--git-dir=");
    argument.push(path);
    argument
}

fn run_git(
    arguments: impl IntoIterator<Item = OsString>,
    input: &[u8],
) -> Result<Vec<u8>, StorageBootstrapError> {
    let program = trusted_git_program().ok_or(StorageBootstrapError::GitProgramUnavailable)?;
    let trusted_path = program.parent().ok_or(StorageBootstrapError::GitProtocol)?;
    let mut command = Command::new(&program);
    command
        .args([
            "-c",
            "core.fsync=all",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.autocrlf=false",
            "-c",
            "core.eol=lf",
            "-c",
            "commit.gpgSign=false",
            "-c",
            "tag.gpgSign=false",
        ])
        .args(arguments)
        .env_clear()
        .env("PATH", trusted_path)
        .env("HOME", "/var/empty")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_OPTIONAL_LOCKS", "0")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().map_err(StorageBootstrapError::Io)?;
    child
        .stdin
        .take()
        .ok_or(StorageBootstrapError::GitProtocol)?
        .write_all(input)
        .map_err(StorageBootstrapError::Io)?;
    let result = child
        .wait_with_output()
        .map_err(StorageBootstrapError::Io)?;
    if result.stdout.len() > MAXIMUM_GIT_OUTPUT_BYTES
        || result.stderr.len() > MAXIMUM_GIT_OUTPUT_BYTES
    {
        return Err(StorageBootstrapError::GitOutputTooLarge);
    }
    if result.status.success() {
        Ok(result.stdout)
    } else {
        let diagnostic = String::from_utf8_lossy(&result.stderr).trim().to_owned();
        Err(StorageBootstrapError::GitFailed(diagnostic))
    }
}

fn parse_git_line(bytes: &[u8]) -> Result<&str, StorageBootstrapError> {
    let value = std::str::from_utf8(bytes)
        .map_err(|_| StorageBootstrapError::GitProtocol)?
        .trim();
    if value.is_empty() || value.chars().any(char::is_whitespace) {
        Err(StorageBootstrapError::GitProtocol)
    } else {
        Ok(value)
    }
}

fn validate_initialized(
    settings: &WorkerSettings,
    runtime_directory: &Path,
    ids: StorageIdentities,
) -> Result<(), StorageBootstrapError> {
    validate_durable_initialized(settings, ids)?;
    validate_runtime_initialized(runtime_directory, ids)
}

fn validate_durable_initialized(
    settings: &WorkerSettings,
    ids: StorageIdentities,
) -> Result<(), StorageBootstrapError> {
    let storage_root = common_storage_root(settings)?;
    validate_storage_directory_no_posix_acl(&storage_root)
        .map_err(|error| StorageBootstrapError::Permissions(storage_root.clone(), error))?;
    require_directory_metadata(
        &storage_root,
        ids.administrative_owner,
        ids.queue_group,
        0o751,
    )?;
    require_directory_metadata(
        settings.queue_root(),
        ids.queue_owner,
        ids.queue_group,
        0o2770,
    )?;
    require_directory_metadata(
        settings.repository_root(),
        ids.worker_owner,
        ids.gateway_group,
        0o2750,
    )?;
    require_directory_metadata(
        settings.content_root(),
        ids.worker_owner,
        ids.gateway_group,
        0o2750,
    )?;
    require_directory_metadata(
        settings.work_root(),
        ids.worker_owner,
        ids.worker_group,
        0o750,
    )?;
    require_directory_metadata(
        settings.release_root(),
        ids.worker_owner,
        ids.worker_group,
        0o750,
    )?;
    validate_queue_tree(
        settings.queue_root(),
        ids.queue_owner,
        ids.worker_owner,
        ids.queue_group,
    )
    .map_err(|error| StorageBootstrapError::Permissions(settings.queue_root().into(), error))?;
    validate_repository_tree(
        settings.repository_root(),
        ids.worker_owner,
        ids.gateway_group,
        Mode::from_bits_truncate(0o2750),
        Mode::from_bits_truncate(0o640),
    )
    .map_err(|error| {
        StorageBootstrapError::Permissions(settings.repository_root().into(), error)
    })?;
    validate_storage_tree(
        settings.content_root(),
        ids.worker_owner,
        ids.gateway_group,
        Mode::from_bits_truncate(0o2750),
        Mode::from_bits_truncate(0o640),
    )
    .map_err(|error| StorageBootstrapError::Permissions(settings.content_root().into(), error))?;
    validate_storage_tree(
        settings.work_root(),
        ids.worker_owner,
        ids.worker_group,
        Mode::from_bits_truncate(0o750),
        Mode::from_bits_truncate(0o640),
    )
    .map_err(|error| StorageBootstrapError::Permissions(settings.work_root().into(), error))?;
    validate_release_tree(
        settings.release_root(),
        ids.worker_owner,
        ids.worker_group,
        Mode::from_bits_truncate(0o750),
        Mode::from_bits_truncate(0o640),
    )
    .map_err(|error| StorageBootstrapError::Permissions(settings.release_root().into(), error))?;
    QueueReader::open_until(settings.queue_root().to_path_buf(), None)
        .map_err(|error| StorageBootstrapError::Component("queue validation", error.to_string()))?;
    CommittedStore::open(
        settings.repository_root(),
        settings.content_root(),
        settings.official_branch(),
    )
    .map_err(|error| {
        StorageBootstrapError::Component("repository validation", error.to_string())
    })?;
    GitRepository::open_existing(
        settings.repository_root(),
        settings.content_root(),
        settings.work_root(),
        settings.official_branch(),
        settings.identity().clone(),
    )
    .map_err(|error| {
        StorageBootstrapError::Component("repository binding validation", error.to_string())
    })?;
    ReleaseReader::open(settings.release_root(), ReleasePolicy::default()).map_err(|error| {
        StorageBootstrapError::Component("release validation", error.to_string())
    })?;
    Ok(())
}

fn validate_runtime_initialized(
    runtime_directory: &Path,
    ids: StorageIdentities,
) -> Result<(), StorageBootstrapError> {
    require_directory_metadata(
        runtime_directory,
        ids.queue_owner,
        ids.ingress_group,
        0o2750,
    )?;
    validate_storage_tree(
        runtime_directory,
        ids.queue_owner,
        ids.ingress_group,
        Mode::from_bits_truncate(0o2750),
        Mode::from_bits_truncate(0o640),
    )
    .map_err(|error| StorageBootstrapError::Permissions(runtime_directory.into(), error))
}

fn require_directory_metadata(
    path: &Path,
    owner: Uid,
    group: Gid,
    mode: u32,
) -> Result<(), StorageBootstrapError> {
    let metadata = fs::symlink_metadata(path).map_err(StorageBootstrapError::Io)?;
    if !metadata.file_type().is_dir()
        || metadata.uid() != owner.as_raw()
        || metadata.gid() != group.as_raw()
        || metadata.permissions().mode() & 0o7777 != mode
    {
        return Err(StorageBootstrapError::InvalidInitializedPath(
            path.to_path_buf(),
        ));
    }
    Ok(())
}

fn read_marker(
    path: &Path,
    owner: Uid,
    group: Gid,
) -> Result<BootstrapMarker, StorageBootstrapError> {
    let metadata = fs::symlink_metadata(path).map_err(StorageBootstrapError::Io)?;
    if !metadata.file_type().is_file()
        || metadata.nlink() != 1
        || metadata.uid() != owner.as_raw()
        || metadata.gid() != group.as_raw()
        || metadata.permissions().mode() & 0o7777 != 0o444
        || metadata.len() > MAXIMUM_MARKER_BYTES
    {
        return Err(StorageBootstrapError::InvalidMarker);
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)
        .map_err(StorageBootstrapError::Io)?
        .take(MAXIMUM_MARKER_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(StorageBootstrapError::Io)?;
    if bytes.len() as u64 > MAXIMUM_MARKER_BYTES {
        return Err(StorageBootstrapError::InvalidMarker);
    }
    let marker: BootstrapMarker =
        serde_json::from_slice(&bytes).map_err(StorageBootstrapError::MarkerJson)?;
    if marker.schema_version != MARKER_VERSION {
        return Err(StorageBootstrapError::InvalidMarker);
    }
    Ok(marker)
}

fn write_marker(
    path: &Path,
    marker: &BootstrapMarker,
    owner: Uid,
    group: Gid,
) -> Result<(), StorageBootstrapError> {
    let parent = path.parent().ok_or(StorageBootstrapError::StorageLayout)?;
    let temporary = parent.join(format!(".{MARKER_NAME}.{}.tmp", Ulid::generate()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o400)
        .open(&temporary)
        .map_err(StorageBootstrapError::Io)?;
    serde_json::to_writer(&mut file, marker).map_err(StorageBootstrapError::MarkerJson)?;
    file.write_all(b"\n").map_err(StorageBootstrapError::Io)?;
    fchown(&file, Some(owner), Some(group))
        .map_err(|error| StorageBootstrapError::Io(io::Error::from_raw_os_error(error as i32)))?;
    file.set_permissions(fs::Permissions::from_mode(0o444))
        .map_err(StorageBootstrapError::Io)?;
    file.sync_all().map_err(StorageBootstrapError::Io)?;
    fs::rename(&temporary, path).map_err(StorageBootstrapError::Io)?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(StorageBootstrapError::Io)
}

fn sync_storage_filesystem(path: &Path) -> Result<(), StorageBootstrapError> {
    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC)
        .open(path)
        .map_err(StorageBootstrapError::Io)?;
    nix::unistd::syncfs(&directory)
        .map_err(|error| StorageBootstrapError::Io(io::Error::from_raw_os_error(error as i32)))
}

fn path_exists(path: &Path) -> Result<bool, StorageBootstrapError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(StorageBootstrapError::Io(error)),
    }
}

fn sync_parent(path: &Path) -> Result<(), StorageBootstrapError> {
    let parent = path.parent().ok_or(StorageBootstrapError::StorageLayout)?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(StorageBootstrapError::Io)
}

fn write_output(
    output: &mut impl Write,
    status: &'static str,
) -> Result<(), StorageBootstrapError> {
    serde_json::to_writer(output.by_ref(), &BootstrapOutput { status })
        .map_err(StorageBootstrapError::MarkerJson)?;
    output.write_all(b"\n").map_err(StorageBootstrapError::Io)?;
    output.flush().map_err(StorageBootstrapError::Io)
}

#[derive(Debug)]
pub(crate) enum StorageBootstrapError {
    RootRequired,
    Config(WorkerConfigError),
    Identity(StorageMigrationError),
    UnsafeServiceIdentities,
    StorageLayout,
    StorageBusy,
    InvalidRuntimeDirectory,
    PartialInitialization(PathBuf),
    UnsafePath(PathBuf),
    UntrustedAncestry(PathBuf),
    MarkerMismatch,
    InvalidMarker,
    InvalidInitializedPath(PathBuf),
    Component(&'static str, String),
    GitProgramUnavailable,
    GitFailed(String),
    GitProtocol,
    GitOutputTooLarge,
    Permissions(PathBuf, StorageMigrationError),
    Attestation(PathAttestationError),
    MarkerJson(serde_json::Error),
    Io(io::Error),
}

impl fmt::Display for StorageBootstrapError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RootRequired => formatter.write_str("storage bootstrap must run as root"),
            Self::Config(error) => write!(formatter, "invalid Worker configuration: {error}"),
            Self::Identity(error) => write!(formatter, "invalid storage identity: {error}"),
            Self::UnsafeServiceIdentities => formatter.write_str(
                "storage bootstrap service UIDs and role GIDs must be non-root and distinct",
            ),
            Self::StorageLayout => formatter.write_str(
                "storage bootstrap requires all five storage paths to be direct children of one non-root directory",
            ),
            Self::StorageBusy => formatter.write_str("another storage bootstrap is running"),
            Self::InvalidRuntimeDirectory => {
                formatter.write_str("runtime directory must be an absolute path outside durable storage")
            }
            Self::PartialInitialization(path) => write!(
                formatter,
                "storage bootstrap found nonempty storage without a completion marker: {}",
                path.display()
            ),
            Self::UnsafePath(path) => write!(
                formatter,
                "storage bootstrap path is not a real directory: {}",
                path.display()
            ),
            Self::UntrustedAncestry(path) => write!(
                formatter,
                "storage bootstrap path has aliased or writable ancestry: {}",
                path.display()
            ),
            Self::MarkerMismatch => formatter.write_str(
                "storage bootstrap marker does not match the requested configuration or identities",
            ),
            Self::InvalidMarker => formatter.write_str("storage bootstrap marker is invalid"),
            Self::InvalidInitializedPath(path) => write!(
                formatter,
                "initialized storage metadata is invalid: {}",
                path.display()
            ),
            Self::Component(component, error) => write!(formatter, "{component} failed: {error}"),
            Self::GitProgramUnavailable => {
                formatter.write_str("no trusted Git executable is available")
            }
            Self::GitFailed(error) if error.is_empty() => {
                formatter.write_str("storage bootstrap Git command failed")
            }
            Self::GitFailed(error) => write!(formatter, "storage bootstrap Git command failed: {error}"),
            Self::GitProtocol => formatter.write_str("storage bootstrap Git output is invalid"),
            Self::GitOutputTooLarge => {
                formatter.write_str("storage bootstrap Git output exceeded its limit")
            }
            Self::Permissions(path, error) => write!(
                formatter,
                "storage permission setup failed for {}: {error}",
                path.display()
            ),
            Self::Attestation(error) => {
                write!(formatter, "storage path attestation failed: {error}")
            }
            Self::MarkerJson(error) => write!(formatter, "storage bootstrap JSON failed: {error}"),
            Self::Io(error) => write!(formatter, "storage bootstrap I/O failed: {error}"),
        }
    }
}

impl std::error::Error for StorageBootstrapError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Config(error) => Some(error),
            Self::Identity(error) | Self::Permissions(_, error) => Some(error),
            Self::Attestation(error) => Some(error),
            Self::MarkerJson(error) => Some(error),
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
#[path = "storage_bootstrap_tests.rs"]
mod tests;

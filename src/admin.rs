use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use agent_knowledge_worker::{
    OperationalStatusError, ReleaseMaintenanceError, WorkerConfigError, WorkerSettings,
    inspect_operational_status, retain_derived_releases,
};
use time::OffsetDateTime;

#[cfg(target_os = "linux")]
use std::ffi::{CStr, CString, OsStr};
#[cfg(target_os = "linux")]
use std::os::unix::ffi::OsStrExt;
#[cfg(target_os = "linux")]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

#[cfg(target_os = "linux")]
use agent_knowledge_core::{PathAttestation, PathAttestationError, RequestId};

#[cfg(target_os = "linux")]
use nix::dir::Dir;
#[cfg(target_os = "linux")]
use nix::fcntl::{OFlag, OpenHow, ResolveFlag, openat, openat2};
#[cfg(target_os = "linux")]
use nix::sys::stat::{Mode, fchmod};
#[cfg(target_os = "linux")]
use nix::unistd::{Gid, Group, Uid, User, dup, fchown};
#[cfg(target_os = "linux")]
use sha2::{Digest, Sha256};
#[cfg(target_os = "linux")]
use ulid::Ulid;
#[cfg(target_os = "linux")]
use xattr::FileExt as _;

#[cfg(target_os = "linux")]
const MAXIMUM_MIGRATION_DEPTH: usize = 128;
#[cfg(target_os = "linux")]
const MAXIMUM_FD_INFO_BYTES: u64 = 16 * 1024;
#[cfg(target_os = "linux")]
const MAXIMUM_MIGRATION_ENTRIES: u64 = 1_000_000;
#[cfg(target_os = "linux")]
const MAXIMUM_MIGRATION_PATH_BYTES: u64 = 512 * 1024 * 1024;
#[cfg(target_os = "linux")]
const POSIX_ACL_XATTRS: [&str; 2] = ["system.posix_acl_access", "system.posix_acl_default"];

pub(crate) fn status<W>(
    config: &Path,
    maximum_queue_entries: usize,
    timeout: Duration,
    mut output: W,
) -> Result<(), AdminStatusError>
where
    W: Write,
{
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or(AdminStatusError::DeadlineOverflow)?;
    let settings = WorkerSettings::load(config).map_err(AdminStatusError::Config)?;
    let status = inspect_operational_status(
        &settings,
        maximum_queue_entries,
        Some(deadline),
        OffsetDateTime::now_utc(),
    )
    .map_err(AdminStatusError::Inspect)?;
    serde_json::to_writer(&mut output, &status).map_err(AdminStatusError::Json)?;
    output.write_all(b"\n").map_err(AdminStatusError::Io)?;
    output.flush().map_err(AdminStatusError::Io)
}

pub(crate) fn prune_releases<W>(
    config: &Path,
    dry_run: bool,
    timeout: Duration,
    mut output: W,
) -> Result<(), AdminRetentionError>
where
    W: Write,
{
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or(AdminRetentionError::DeadlineOverflow)?;
    let settings = WorkerSettings::load(config).map_err(AdminRetentionError::Config)?;
    let outcome = retain_derived_releases(&settings, dry_run, Some(deadline))
        .map_err(AdminRetentionError::Retain)?;
    serde_json::to_writer(&mut output, &outcome).map_err(AdminRetentionError::Json)?;
    output.write_all(b"\n").map_err(AdminRetentionError::Io)?;
    output.flush().map_err(AdminRetentionError::Io)
}

#[cfg(target_os = "linux")]
pub(crate) struct StorageMigration<'a> {
    pub(crate) queue_root: &'a Path,
    pub(crate) git_directory: &'a Path,
    pub(crate) content_root: &'a Path,
    pub(crate) queue_owner: &'a OsStr,
    pub(crate) queue_group: &'a OsStr,
    pub(crate) gateway_group: &'a OsStr,
}

#[cfg(target_os = "linux")]
pub(crate) fn migrate_v1_storage_permissions(
    settings: StorageMigration<'_>,
    mut output: impl Write,
) -> Result<(), StorageMigrationError> {
    if !Uid::effective().is_root() {
        return Err(StorageMigrationError::RootRequired);
    }
    let queue_owner = resolve_user(settings.queue_owner)?;
    let queue_group = resolve_group(settings.queue_group)?;
    let gateway_group = resolve_group(settings.gateway_group)?;
    migrate_v1_storage_with_ids(
        settings.queue_root,
        settings.git_directory,
        settings.content_root,
        queue_owner,
        queue_group,
        gateway_group,
        &mut output,
    )
}

#[cfg(target_os = "linux")]
fn migrate_v1_storage_with_ids(
    queue_root: &Path,
    git_directory: &Path,
    content_root: &Path,
    queue_owner: Uid,
    queue_group: Gid,
    gateway_group: Gid,
    mut output: impl Write,
) -> Result<(), StorageMigrationError> {
    let queue = MigrationRoot::open(queue_root)?;
    let git = MigrationRoot::open(git_directory)?;
    let content = MigrationRoot::open(content_root)?;
    ensure_disjoint_roots([&queue, &git, &content])?;

    let queue_lock = open_regular_beneath(&queue.file, Path::new(".locks/queue.lock"))?;
    let worker_lock =
        open_regular_beneath(&queue.file, Path::new(".locks/repository-writer.lock"))?;
    queue_lock
        .try_lock()
        .map_err(|_| StorageMigrationError::QueueBusy)?;
    worker_lock
        .try_lock()
        .map_err(|_| StorageMigrationError::QueueBusy)?;
    require_empty_directory(&queue.file, Path::new("incoming"))?;
    require_empty_directory(&queue.file, Path::new("quarantine"))?;
    let mut migration_budget = MigrationBudget::new();
    preflight_directory(&queue.file, 0, Path::new(""), &mut migration_budget)?;
    preflight_directory(&git.file, 0, Path::new(""), &mut migration_budget)?;
    preflight_directory(&content.file, 0, Path::new(""), &mut migration_budget)?;

    let mut queue_fingerprint = TreeFingerprintBuilder::new();
    migrate_directory(
        &queue.file,
        TreePermissions {
            owner: None,
            group: queue_group,
            directory_mode: Mode::from_bits_truncate(0o2770),
            file_mode: Mode::from_bits_truncate(0o660),
        },
        0,
        Path::new(""),
        &mut queue_fingerprint,
    )?;
    let queue_fingerprint = queue_fingerprint.finish();
    let mut git_fingerprint = TreeFingerprintBuilder::new();
    migrate_directory(
        &git.file,
        TreePermissions {
            owner: None,
            group: gateway_group,
            directory_mode: Mode::from_bits_truncate(0o2750),
            file_mode: Mode::from_bits_truncate(0o640),
        },
        0,
        Path::new(""),
        &mut git_fingerprint,
    )?;
    let git_fingerprint = git_fingerprint.finish();
    let mut content_fingerprint = TreeFingerprintBuilder::new();
    migrate_directory(
        &content.file,
        TreePermissions {
            owner: None,
            group: gateway_group,
            directory_mode: Mode::from_bits_truncate(0o2750),
            file_mode: Mode::from_bits_truncate(0o640),
        },
        0,
        Path::new(""),
        &mut content_fingerprint,
    )?;
    let content_fingerprint = content_fingerprint.finish();

    for relative in [
        "",
        ".locks",
        "incoming",
        "quarantine",
        "worker-tmp",
        "pending",
        "processing",
        "completed",
        "failed",
    ] {
        let directory = if relative.is_empty() {
            queue.file.try_clone().map_err(StorageMigrationError::Io)?
        } else {
            open_directory_beneath(&queue.file, Path::new(relative))?
        };
        set_identity_and_mode(
            &directory,
            Some(queue_owner),
            queue_group,
            Mode::from_bits_truncate(0o2770),
        )?;
    }
    set_identity_and_mode(
        &queue_lock,
        Some(queue_owner),
        queue_group,
        Mode::from_bits_truncate(0o660),
    )?;
    set_identity_and_mode(
        &worker_lock,
        Some(queue_owner),
        queue_group,
        Mode::from_bits_truncate(0o660),
    )?;
    verify_fingerprint(&queue.file, &queue_fingerprint)?;
    verify_fingerprint(&git.file, &git_fingerprint)?;
    verify_fingerprint(&content.file, &content_fingerprint)?;
    queue.file.sync_all().map_err(StorageMigrationError::Io)?;
    git.file.sync_all().map_err(StorageMigrationError::Io)?;
    content.file.sync_all().map_err(StorageMigrationError::Io)?;
    queue.revalidate()?;
    git.revalidate()?;
    content.revalidate()?;
    output
        .write_all(b"{\"status\":\"completed\"}\n")
        .map_err(StorageMigrationError::Io)
}

#[cfg(target_os = "linux")]
pub(crate) fn normalize_storage_tree(
    root: &Path,
    owner: Uid,
    group: Gid,
    directory_mode: Mode,
    file_mode: Mode,
) -> Result<(), StorageMigrationError> {
    let root = MigrationRoot::open(root)?;
    let mut budget = MigrationBudget::new();
    preflight_directory(&root.file, 0, Path::new(""), &mut budget)?;
    let mut fingerprint = TreeFingerprintBuilder::new();
    migrate_directory(
        &root.file,
        TreePermissions {
            owner: Some(owner),
            group,
            directory_mode,
            file_mode,
        },
        0,
        Path::new(""),
        &mut fingerprint,
    )?;
    let fingerprint = fingerprint.finish();
    verify_fingerprint(&root.file, &fingerprint)?;
    root.file.sync_all().map_err(StorageMigrationError::Io)?;
    root.revalidate()
}

#[cfg(target_os = "linux")]
pub(crate) fn validate_bootstrap_source_tree(root: &Path) -> Result<(), StorageMigrationError> {
    let root = MigrationRoot::open(root)?;
    let mut budget = MigrationBudget::new();
    preflight_directory(&root.file, 0, Path::new(""), &mut budget)?;
    root.revalidate()
}

#[cfg(target_os = "linux")]
pub(crate) fn validate_storage_directory_no_posix_acl(
    path: &Path,
) -> Result<(), StorageMigrationError> {
    let root = MigrationRoot::open(path)?;
    require_no_posix_acl(&root.file, Path::new(""))?;
    root.revalidate()
}

#[cfg(target_os = "linux")]
pub(crate) fn validate_storage_tree(
    root: &Path,
    owner: Uid,
    group: Gid,
    directory_mode: Mode,
    file_mode: Mode,
) -> Result<(), StorageMigrationError> {
    let root = MigrationRoot::open(root)?;
    let mut budget = MigrationBudget::new();
    preflight_directory(&root.file, 0, Path::new(""), &mut budget)?;
    let permissions = TreePermissions {
        owner: Some(owner),
        group,
        directory_mode,
        file_mode,
    };
    let mut fingerprint = TreeFingerprintBuilder::new();
    validate_directory_permissions(
        &root.file,
        permissions,
        0,
        Path::new(""),
        &mut fingerprint,
        TreeValidation::strict(),
    )?;
    let fingerprint = fingerprint.finish();
    verify_fingerprint(&root.file, &fingerprint)?;
    root.revalidate()
}

#[cfg(target_os = "linux")]
pub(crate) fn validate_repository_tree(
    root: &Path,
    owner: Uid,
    group: Gid,
    directory_mode: Mode,
    file_mode: Mode,
) -> Result<(), StorageMigrationError> {
    let root = MigrationRoot::open(root)?;
    let mut budget = MigrationBudget::new();
    preflight_directory(&root.file, 0, Path::new(""), &mut budget)?;
    let permissions = TreePermissions {
        owner: Some(owner),
        group,
        directory_mode,
        file_mode,
    };
    let mut fingerprint = TreeFingerprintBuilder::new();
    validate_directory_permissions(
        &root.file,
        permissions,
        0,
        Path::new(""),
        &mut fingerprint,
        TreeValidation {
            allow_read_only_files: true,
            ..TreeValidation::strict()
        },
    )?;
    let fingerprint = fingerprint.finish();
    verify_fingerprint(&root.file, &fingerprint)?;
    root.revalidate()
}

#[cfg(target_os = "linux")]
pub(crate) fn validate_release_tree(
    root: &Path,
    owner: Uid,
    group: Gid,
    directory_mode: Mode,
    file_mode: Mode,
) -> Result<(), StorageMigrationError> {
    let root = MigrationRoot::open(root)?;
    let mut budget = MigrationBudget::new();
    preflight_release_directory(&root.file, 0, Path::new(""), &mut budget)?;
    let permissions = TreePermissions {
        owner: Some(owner),
        group,
        directory_mode,
        file_mode,
    };
    let mut fingerprint = TreeFingerprintBuilder::new();
    validate_directory_permissions(
        &root.file,
        permissions,
        0,
        Path::new(""),
        &mut fingerprint,
        TreeValidation {
            allow_symlinks: true,
            ..TreeValidation::strict()
        },
    )?;
    let fingerprint = fingerprint.finish();
    verify_release_fingerprint(&root.file, &fingerprint)?;
    root.revalidate()
}

#[cfg(target_os = "linux")]
pub(crate) fn validate_queue_tree(
    root: &Path,
    queue_owner: Uid,
    worker_owner: Uid,
    group: Gid,
) -> Result<(), StorageMigrationError> {
    let root = MigrationRoot::open(root)?;
    let mut budget = MigrationBudget::new();
    preflight_directory(&root.file, 0, Path::new(""), &mut budget)?;
    let permissions = TreePermissions {
        owner: Some(queue_owner),
        group,
        directory_mode: Mode::from_bits_truncate(0o2770),
        file_mode: Mode::from_bits_truncate(0o660),
    };
    let mut fingerprint = TreeFingerprintBuilder::new();
    validate_directory_permissions(
        &root.file,
        permissions,
        0,
        Path::new(""),
        &mut fingerprint,
        TreeValidation {
            alternate_file: Some((worker_owner, Mode::from_bits_truncate(0o640))),
            ..TreeValidation::strict()
        },
    )?;
    let fingerprint = fingerprint.finish();
    verify_fingerprint(&root.file, &fingerprint)?;
    root.revalidate()
}

#[cfg(target_os = "linux")]
pub(crate) fn validate_same_storage_mount(
    root: &Path,
    descendants: &[&Path],
) -> Result<(), StorageMigrationError> {
    let root = MigrationRoot::open(root)?;
    let expected = linux_mount_id(&root.file)?;
    for descendant in descendants {
        let descendant = MigrationRoot::open(descendant)?;
        if linux_mount_id(&descendant.file)? != expected {
            return Err(StorageMigrationError::UnsafeEntry(descendant.configured));
        }
        descendant.revalidate()?;
    }
    root.revalidate()
}

#[cfg(target_os = "linux")]
pub(crate) fn validate_storage_file_mount(
    root: &Path,
    file: &Path,
) -> Result<(), StorageMigrationError> {
    let root = MigrationRoot::open(root)?;
    let relative = file
        .strip_prefix(&root.configured)
        .map_err(|_| StorageMigrationError::UnsafeEntry(file.to_path_buf()))?;
    let pinned = open_regular_beneath(&root.file, relative)?;
    if linux_mount_id(&pinned)? != linux_mount_id(&root.file)? {
        return Err(StorageMigrationError::UnsafeEntry(file.to_path_buf()));
    }
    require_no_posix_acl(&pinned, relative)?;
    root.revalidate()
}

#[cfg(target_os = "linux")]
pub(crate) fn normalize_storage_directory(
    path: &Path,
    owner: Uid,
    group: Gid,
    mode: Mode,
) -> Result<(), StorageMigrationError> {
    normalized_absolute_suffix(path)?;
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC)
        .open(path)
        .map_err(StorageMigrationError::Io)?;
    let attestation =
        PathAttestation::capture(path, &file).map_err(StorageMigrationError::Attestation)?;
    if attestation.path() != path {
        return Err(StorageMigrationError::InvalidRoot(path.to_path_buf()));
    }
    require_no_posix_acl(&file, Path::new(""))?;
    set_identity_and_mode(&file, Some(owner), group, mode)?;
    require_no_posix_acl(&file, Path::new(""))?;
    file.sync_all().map_err(StorageMigrationError::Io)?;
    let observed =
        PathAttestation::capture(path, &file).map_err(StorageMigrationError::Attestation)?;
    if observed.path() == path && attestation.matches_destination(&observed) {
        Ok(())
    } else {
        Err(StorageMigrationError::TreeChanged(path.to_path_buf()))
    }
}

#[cfg(target_os = "linux")]
pub(crate) fn resolve_user(value: &OsStr) -> Result<Uid, StorageMigrationError> {
    if let Some(identifier) = value.to_str().and_then(|value| value.parse::<u32>().ok()) {
        return Ok(Uid::from_raw(identifier));
    }
    let name = value
        .to_str()
        .ok_or_else(|| StorageMigrationError::UnknownIdentity(value.to_os_string()))?;
    User::from_name(name)
        .map_err(nix_io_error)?
        .map(|user| user.uid)
        .ok_or_else(|| StorageMigrationError::UnknownIdentity(value.to_os_string()))
}

#[cfg(target_os = "linux")]
pub(crate) fn resolve_group(value: &OsStr) -> Result<Gid, StorageMigrationError> {
    if let Some(identifier) = value.to_str().and_then(|value| value.parse::<u32>().ok()) {
        return Ok(Gid::from_raw(identifier));
    }
    let name = value
        .to_str()
        .ok_or_else(|| StorageMigrationError::UnknownIdentity(value.to_os_string()))?;
    Group::from_name(name)
        .map_err(nix_io_error)?
        .map(|group| group.gid)
        .ok_or_else(|| StorageMigrationError::UnknownIdentity(value.to_os_string()))
}

#[cfg(target_os = "linux")]
struct MigrationRoot {
    configured: PathBuf,
    file: File,
    attestation: PathAttestation,
}

#[cfg(target_os = "linux")]
impl MigrationRoot {
    fn open(path: &Path) -> Result<Self, StorageMigrationError> {
        let relative = normalized_absolute_suffix(path)?;
        let filesystem_root = OpenOptions::new()
            .read(true)
            .custom_flags(nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC)
            .open("/")
            .map_err(StorageMigrationError::Io)?;
        let file = open_relative(
            &filesystem_root,
            &relative,
            OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
            false,
        )?;
        let attestation =
            PathAttestation::capture(path, &file).map_err(StorageMigrationError::Attestation)?;
        if attestation.path() != path {
            return Err(StorageMigrationError::InvalidRoot(path.to_path_buf()));
        }
        Ok(Self {
            configured: path.to_path_buf(),
            file,
            attestation,
        })
    }

    fn revalidate(&self) -> Result<(), StorageMigrationError> {
        let observed = PathAttestation::capture(&self.configured, &self.file)
            .map_err(StorageMigrationError::Attestation)?;
        if observed.path() == self.configured && self.attestation.matches_destination(&observed) {
            Ok(())
        } else {
            Err(StorageMigrationError::TreeChanged(self.configured.clone()))
        }
    }
}

#[cfg(target_os = "linux")]
fn normalized_absolute_suffix(path: &Path) -> Result<PathBuf, StorageMigrationError> {
    let mut components = path.components();
    if components.next() != Some(std::path::Component::RootDir) {
        return Err(StorageMigrationError::InvalidRoot(path.to_path_buf()));
    }
    let mut relative = PathBuf::new();
    for component in components {
        let std::path::Component::Normal(name) = component else {
            return Err(StorageMigrationError::InvalidRoot(path.to_path_buf()));
        };
        relative.push(name);
    }
    if relative.as_os_str().is_empty() || Path::new("/").join(&relative) != path {
        return Err(StorageMigrationError::InvalidRoot(path.to_path_buf()));
    }
    Ok(relative)
}

#[cfg(target_os = "linux")]
fn ensure_disjoint_roots(roots: [&MigrationRoot; 3]) -> Result<(), StorageMigrationError> {
    for (index, root) in roots.iter().enumerate() {
        if roots.iter().enumerate().any(|(other_index, other)| {
            index != other_index
                && (root.attestation.is_within(&other.attestation)
                    || other.attestation.is_within(&root.attestation))
        }) {
            return Err(StorageMigrationError::OverlappingRoots);
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn open_directory_beneath(root: &File, path: &Path) -> Result<File, StorageMigrationError> {
    open_relative(
        root,
        path,
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        true,
    )
}

#[cfg(target_os = "linux")]
fn open_regular_beneath(root: &File, path: &Path) -> Result<File, StorageMigrationError> {
    let pinned = open_relative(
        root,
        path,
        OFlag::O_PATH | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        true,
    )?;
    let identity = require_regular(&pinned, path)?;
    let file = open_relative(
        root,
        path,
        OFlag::O_RDONLY | OFlag::O_NONBLOCK | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        true,
    )?;
    if object_identity(&file)? != identity {
        return Err(StorageMigrationError::TreeChanged(path.to_path_buf()));
    }
    Ok(file)
}

#[cfg(target_os = "linux")]
fn open_relative(
    root: &File,
    path: &Path,
    flags: OFlag,
    no_xdev: bool,
) -> Result<File, StorageMigrationError> {
    validate_relative_path(path)?;
    let resolve = ResolveFlag::RESOLVE_BENEATH
        | ResolveFlag::RESOLVE_NO_SYMLINKS
        | if no_xdev {
            ResolveFlag::RESOLVE_NO_XDEV
        } else {
            ResolveFlag::empty()
        };
    let how = OpenHow::new().flags(flags).resolve(resolve);
    match openat2(root, path, how) {
        Ok(file) => Ok(File::from(file)),
        Err(nix::errno::Errno::ENOSYS | nix::errno::Errno::EPERM) => {
            open_relative_with_openat(root, path, flags, no_xdev)
        }
        Err(error) => Err(nix_io_error(error)),
    }
}

#[cfg(target_os = "linux")]
fn open_relative_with_openat(
    root: &File,
    path: &Path,
    final_flags: OFlag,
    no_xdev: bool,
) -> Result<File, StorageMigrationError> {
    let expected_mount = no_xdev.then(|| linux_mount_id(root)).transpose()?;
    let mut directory = dup(root).map(File::from).map_err(nix_io_error)?;
    let mut components = path.components().peekable();
    while let Some(component) = components.next() {
        let std::path::Component::Normal(name) = component else {
            return Err(StorageMigrationError::UnsafeEntry(path.to_path_buf()));
        };
        let flags = if components.peek().is_some() {
            OFlag::O_PATH | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC
        } else {
            final_flags | OFlag::O_NOFOLLOW
        };
        let opened = openat(&directory, name, flags, Mode::empty())
            .map(File::from)
            .map_err(nix_io_error)?;
        if let Some(expected) = expected_mount
            && linux_mount_id(&opened)? != expected
        {
            return Err(StorageMigrationError::UnsafeEntry(path.to_path_buf()));
        }
        directory = opened;
    }
    Ok(directory)
}

#[cfg(target_os = "linux")]
fn validate_relative_path(path: &Path) -> Result<(), StorageMigrationError> {
    if path.as_os_str().is_empty()
        || path
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        Err(StorageMigrationError::UnsafeEntry(path.to_path_buf()))
    } else {
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn require_empty_directory(root: &File, path: &Path) -> Result<(), StorageMigrationError> {
    let directory = open_directory_beneath(root, path)?;
    if directory_entries(&directory, path)?.is_empty() {
        Ok(())
    } else {
        Err(StorageMigrationError::DirectoryNotEmpty(path.to_path_buf()))
    }
}

#[cfg(target_os = "linux")]
fn preflight_directory(
    directory: &File,
    depth: usize,
    relative: &Path,
    budget: &mut MigrationBudget,
) -> Result<(), StorageMigrationError> {
    if depth > MAXIMUM_MIGRATION_DEPTH {
        return Err(StorageMigrationError::UnsafeEntry(relative.to_path_buf()));
    }
    require_no_posix_acl(directory, relative)?;
    budget.record(relative)?;
    let cloned = directory.try_clone().map_err(StorageMigrationError::Io)?;
    let mut entries = Dir::from_fd(cloned.into()).map_err(nix_io_error)?;
    for entry in entries.iter() {
        let entry = entry.map_err(nix_io_error)?;
        let name = entry.file_name();
        if matches!(name.to_bytes(), b"." | b"..") {
            continue;
        }
        let name = CString::new(name.to_bytes())
            .map_err(|_| StorageMigrationError::UnsafeEntry(relative.to_path_buf()))?;
        let path = relative.join(OsStr::from_bytes(name.to_bytes()));
        let pinned = open_child_path(directory, &name)?;
        let identity = object_identity(&pinned)?;
        if identity.file_type == nix::libc::S_IFDIR {
            let child = open_child_directory(directory, &name)?;
            if object_identity(&child)? != identity {
                return Err(StorageMigrationError::TreeChanged(path));
            }
            preflight_directory(&child, depth + 1, &path, budget)?;
        } else if identity.file_type == nix::libc::S_IFREG && identity.links == 1 {
            let child = open_child_regular(directory, &name)?;
            if object_identity(&child)? != identity {
                return Err(StorageMigrationError::TreeChanged(path));
            }
            require_no_posix_acl(&child, &path)?;
            budget.record(&path)?;
        } else {
            return Err(StorageMigrationError::UnsafeEntry(path));
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn preflight_release_directory(
    directory: &File,
    depth: usize,
    relative: &Path,
    budget: &mut MigrationBudget,
) -> Result<(), StorageMigrationError> {
    if depth > MAXIMUM_MIGRATION_DEPTH {
        return Err(StorageMigrationError::UnsafeEntry(relative.to_path_buf()));
    }
    require_no_posix_acl(directory, relative)?;
    budget.record(relative)?;
    let cloned = directory.try_clone().map_err(StorageMigrationError::Io)?;
    let mut entries = Dir::from_fd(cloned.into()).map_err(nix_io_error)?;
    for entry in entries.iter() {
        let entry = entry.map_err(nix_io_error)?;
        let name = entry.file_name();
        if matches!(name.to_bytes(), b"." | b"..") {
            continue;
        }
        let name = CString::new(name.to_bytes())
            .map_err(|_| StorageMigrationError::UnsafeEntry(relative.to_path_buf()))?;
        let path = relative.join(OsStr::from_bytes(name.to_bytes()));
        let pinned = open_child_path(directory, &name)?;
        let identity = object_identity(&pinned)?;
        if identity.file_type == nix::libc::S_IFDIR {
            let child = open_child_directory(directory, &name)?;
            if object_identity(&child)? != identity {
                return Err(StorageMigrationError::TreeChanged(path));
            }
            preflight_release_directory(&child, depth + 1, &path, budget)?;
        } else if matches!(identity.file_type, nix::libc::S_IFREG | nix::libc::S_IFLNK)
            && identity.links == 1
        {
            if identity.file_type == nix::libc::S_IFREG {
                let child = open_child_regular(directory, &name)?;
                if object_identity(&child)? != identity {
                    return Err(StorageMigrationError::TreeChanged(path));
                }
                require_no_posix_acl(&child, &path)?;
            }
            budget.record(&path)?;
        } else {
            return Err(StorageMigrationError::UnsafeEntry(path));
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy)]
struct TreePermissions {
    owner: Option<Uid>,
    group: Gid,
    directory_mode: Mode,
    file_mode: Mode,
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy)]
struct TreeValidation {
    allow_read_only_files: bool,
    allow_symlinks: bool,
    alternate_file: Option<(Uid, Mode)>,
}

#[cfg(target_os = "linux")]
impl TreeValidation {
    const fn strict() -> Self {
        Self {
            allow_read_only_files: false,
            allow_symlinks: false,
            alternate_file: None,
        }
    }
}

#[cfg(target_os = "linux")]
fn migrate_directory(
    directory: &File,
    permissions: TreePermissions,
    depth: usize,
    relative: &Path,
    fingerprint: &mut TreeFingerprintBuilder,
) -> Result<(), StorageMigrationError> {
    if depth > MAXIMUM_MIGRATION_DEPTH {
        return Err(StorageMigrationError::UnsafeEntry(relative.to_path_buf()));
    }
    let entries = directory_entries(directory, relative)?;
    for name in &entries {
        let path = relative.join(OsStr::from_bytes(name.to_bytes()));
        let pinned = open_child_path(directory, name)?;
        let identity = object_identity(&pinned)?;
        if identity.file_type == nix::libc::S_IFDIR {
            let child = open_child_directory(directory, name)?;
            if object_identity(&child)? != identity {
                return Err(StorageMigrationError::TreeChanged(path));
            }
            migrate_directory(&child, permissions, depth + 1, &path, fingerprint)?;
        } else if identity.file_type == nix::libc::S_IFREG && identity.links == 1 {
            let child = open_child_regular(directory, name)?;
            if object_identity(&child)? != identity {
                return Err(StorageMigrationError::TreeChanged(path));
            }
            set_regular_identity_and_mode(
                &child,
                permissions.owner,
                permissions.group,
                permissions.file_mode,
                &path,
            )?;
            child.sync_all().map_err(StorageMigrationError::Io)?;
            fingerprint.record(&path, object_identity(&child)?)?;
        } else {
            return Err(StorageMigrationError::UnsafeEntry(path));
        }
    }
    if entries != directory_entries(directory, relative)? {
        return Err(StorageMigrationError::TreeChanged(relative.to_path_buf()));
    }
    set_identity_and_mode(
        directory,
        permissions.owner,
        permissions.group,
        permissions.directory_mode,
    )?;
    directory.sync_all().map_err(StorageMigrationError::Io)?;
    fingerprint.record(relative, object_identity(directory)?)?;
    Ok(())
}

#[cfg(target_os = "linux")]
fn validate_directory_permissions(
    directory: &File,
    permissions: TreePermissions,
    depth: usize,
    relative: &Path,
    fingerprint: &mut TreeFingerprintBuilder,
    validation: TreeValidation,
) -> Result<(), StorageMigrationError> {
    if depth > MAXIMUM_MIGRATION_DEPTH {
        return Err(StorageMigrationError::UnsafeEntry(relative.to_path_buf()));
    }
    let entries = directory_entries(directory, relative)?;
    for name in &entries {
        let path = relative.join(OsStr::from_bytes(name.to_bytes()));
        let pinned = open_child_path(directory, name)?;
        let identity = object_identity(&pinned)?;
        if identity.file_type == nix::libc::S_IFDIR {
            let child = open_child_directory(directory, name)?;
            if object_identity(&child)? != identity {
                return Err(StorageMigrationError::TreeChanged(path));
            }
            validate_directory_permissions(
                &child,
                permissions,
                depth + 1,
                &path,
                fingerprint,
                validation,
            )?;
        } else if identity.file_type == nix::libc::S_IFREG && identity.links == 1 {
            let child = open_child_regular(directory, name)?;
            if object_identity(&child)? != identity {
                return Err(StorageMigrationError::TreeChanged(path));
            }
            require_no_posix_acl(&child, &path)?;
            require_file_metadata(
                &child,
                permissions.owner,
                permissions.group,
                permissions.file_mode,
                &path,
                validation.allow_read_only_files,
                validation.alternate_file,
            )?;
            fingerprint.record(&path, identity)?;
        } else if validation.allow_symlinks
            && identity.file_type == nix::libc::S_IFLNK
            && identity.links == 1
        {
            require_metadata(
                &pinned,
                permissions.owner,
                permissions.group,
                Mode::from_bits_truncate(0o777),
                &path,
                false,
            )?;
            fingerprint.record(&path, identity)?;
        } else {
            return Err(StorageMigrationError::UnsafeEntry(path));
        }
    }
    if entries != directory_entries(directory, relative)? {
        return Err(StorageMigrationError::TreeChanged(relative.to_path_buf()));
    }
    require_no_posix_acl(directory, relative)?;
    require_metadata(
        directory,
        permissions.owner,
        permissions.group,
        permissions.directory_mode,
        relative,
        false,
    )?;
    fingerprint.record(relative, object_identity(directory)?)?;
    Ok(())
}

#[cfg(target_os = "linux")]
fn require_no_posix_acl(file: &File, path: &Path) -> Result<(), StorageMigrationError> {
    for name in POSIX_ACL_XATTRS {
        if file
            .get_xattr(name)
            .map_err(StorageMigrationError::Io)?
            .is_some()
        {
            return Err(StorageMigrationError::PosixAcl(path.to_path_buf()));
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn require_metadata(
    file: &File,
    owner: Option<Uid>,
    group: Gid,
    mode: Mode,
    path: &Path,
    allow_owner_write_missing: bool,
) -> Result<(), StorageMigrationError> {
    let metadata = file.metadata().map_err(StorageMigrationError::Io)?;
    if metadata_matches(&metadata, owner, group, mode, allow_owner_write_missing) {
        Ok(())
    } else {
        Err(StorageMigrationError::UnsafeEntry(path.to_path_buf()))
    }
}

#[cfg(target_os = "linux")]
fn require_file_metadata(
    file: &File,
    owner: Option<Uid>,
    group: Gid,
    mode: Mode,
    path: &Path,
    allow_owner_write_missing: bool,
    alternate: Option<(Uid, Mode)>,
) -> Result<(), StorageMigrationError> {
    let metadata = file.metadata().map_err(StorageMigrationError::Io)?;
    if metadata_matches(&metadata, owner, group, mode, allow_owner_write_missing)
        || (worker_owned_queue_file(path)
            && alternate.is_some_and(|(owner, mode)| {
                metadata_matches(&metadata, Some(owner), group, mode, false)
            }))
    {
        Ok(())
    } else {
        Err(StorageMigrationError::UnsafeEntry(path.to_path_buf()))
    }
}

#[cfg(target_os = "linux")]
fn worker_owned_queue_file(path: &Path) -> bool {
    let mut components = path.iter();
    let Some(first) = components.next() else {
        return false;
    };
    if first == "worker-tmp" {
        let Some(name) = components.next().and_then(|name| name.to_str()) else {
            return false;
        };
        return components.next().is_none() && valid_worker_temporary_name(name);
    }
    if !matches!(
        first.as_encoded_bytes(),
        b"pending" | b"processing" | b"completed" | b"failed"
    ) {
        return false;
    }
    let Some(request_id) = components.next().and_then(|value| value.to_str()) else {
        return false;
    };
    let Some(sidecar) = components.next() else {
        return false;
    };
    components.next().is_none()
        && request_id.parse::<RequestId>().is_ok()
        && matches!(sidecar.as_encoded_bytes(), b"phase.json" | b"result.json")
}

#[cfg(target_os = "linux")]
fn valid_worker_temporary_name(name: &str) -> bool {
    [".phase-", ".result-"].iter().any(|prefix| {
        name.strip_prefix(prefix)
            .and_then(|value| value.parse::<Ulid>().ok().map(|id| (value, id)))
            .is_some_and(|(value, id)| id.to_string() == value)
    })
}

#[cfg(target_os = "linux")]
fn metadata_matches(
    metadata: &std::fs::Metadata,
    owner: Option<Uid>,
    group: Gid,
    mode: Mode,
    allow_owner_write_missing: bool,
) -> bool {
    let observed_mode = metadata.mode() & 0o7777;
    let expected_mode = mode.bits();
    let mode_matches = observed_mode == expected_mode
        || (allow_owner_write_missing && observed_mode == expected_mode & !0o200);
    owner.is_none_or(|owner| metadata.uid() == owner.as_raw())
        && metadata.gid() == group.as_raw()
        && mode_matches
}

#[cfg(target_os = "linux")]
fn directory_entries(
    directory: &File,
    relative: &Path,
) -> Result<Vec<CString>, StorageMigrationError> {
    let cloned = directory.try_clone().map_err(StorageMigrationError::Io)?;
    let mut entries = Dir::from_fd(cloned.into()).map_err(nix_io_error)?;
    let mut names = Vec::new();
    for entry in entries.iter() {
        let entry = entry.map_err(nix_io_error)?;
        let name = entry.file_name().to_bytes();
        if name != b"." && name != b".." {
            if names.len() as u64 >= MAXIMUM_MIGRATION_ENTRIES {
                return Err(StorageMigrationError::MigrationLimitExceeded(
                    relative.to_path_buf(),
                ));
            }
            names.push(
                CString::new(name)
                    .map_err(|_| StorageMigrationError::UnsafeEntry(PathBuf::new()))?,
            );
        }
    }
    names.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    Ok(names)
}

#[cfg(target_os = "linux")]
fn open_child_path(parent: &File, name: &CStr) -> Result<File, StorageMigrationError> {
    open_relative(
        parent,
        Path::new(OsStr::from_bytes(name.to_bytes())),
        OFlag::O_PATH | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        true,
    )
}

#[cfg(target_os = "linux")]
fn open_child_directory(parent: &File, name: &CStr) -> Result<File, StorageMigrationError> {
    open_relative(
        parent,
        Path::new(OsStr::from_bytes(name.to_bytes())),
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        true,
    )
}

#[cfg(target_os = "linux")]
fn open_child_regular(parent: &File, name: &CStr) -> Result<File, StorageMigrationError> {
    open_relative(
        parent,
        Path::new(OsStr::from_bytes(name.to_bytes())),
        OFlag::O_RDONLY | OFlag::O_NONBLOCK | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        true,
    )
}

#[cfg(target_os = "linux")]
fn require_regular(file: &File, path: &Path) -> Result<ObjectIdentity, StorageMigrationError> {
    let identity = object_identity(file)?;
    if identity.file_type == nix::libc::S_IFREG && identity.links == 1 {
        Ok(identity)
    } else {
        Err(StorageMigrationError::UnsafeEntry(path.to_path_buf()))
    }
}

#[cfg(target_os = "linux")]
fn set_regular_identity_and_mode(
    file: &File,
    owner: Option<Uid>,
    group: Gid,
    mode: Mode,
    path: &Path,
) -> Result<(), StorageMigrationError> {
    require_regular(file, path)?;
    set_identity_and_mode(file, owner, group, mode)?;
    require_regular(file, path).map(|_| ())
}

#[cfg(target_os = "linux")]
fn verify_fingerprint(
    root: &File,
    expected: &TreeFingerprint,
) -> Result<(), StorageMigrationError> {
    let mut observed = TreeFingerprintBuilder::new();
    fingerprint_directory(root, 0, Path::new(""), &mut observed, false)?;
    if &observed.finish() == expected {
        Ok(())
    } else {
        Err(StorageMigrationError::TreeChanged(PathBuf::new()))
    }
}

#[cfg(target_os = "linux")]
fn verify_release_fingerprint(
    root: &File,
    expected: &TreeFingerprint,
) -> Result<(), StorageMigrationError> {
    let mut observed = TreeFingerprintBuilder::new();
    fingerprint_directory(root, 0, Path::new(""), &mut observed, true)?;
    if &observed.finish() == expected {
        Ok(())
    } else {
        Err(StorageMigrationError::TreeChanged(PathBuf::new()))
    }
}

#[cfg(target_os = "linux")]
fn fingerprint_directory(
    directory: &File,
    depth: usize,
    relative: &Path,
    fingerprint: &mut TreeFingerprintBuilder,
    allow_symlinks: bool,
) -> Result<(), StorageMigrationError> {
    if depth > MAXIMUM_MIGRATION_DEPTH {
        return Err(StorageMigrationError::UnsafeEntry(relative.to_path_buf()));
    }
    let entries = directory_entries(directory, relative)?;
    for name in &entries {
        let path = relative.join(OsStr::from_bytes(name.to_bytes()));
        let pinned = open_child_path(directory, name)?;
        let identity = object_identity(&pinned)?;
        if identity.file_type == nix::libc::S_IFDIR {
            let child = open_child_directory(directory, name)?;
            if object_identity(&child)? != identity {
                return Err(StorageMigrationError::TreeChanged(path));
            }
            fingerprint_directory(&child, depth + 1, &path, fingerprint, allow_symlinks)?;
        } else if (identity.file_type == nix::libc::S_IFREG
            || (allow_symlinks && identity.file_type == nix::libc::S_IFLNK))
            && identity.links == 1
        {
            fingerprint.record(&path, identity)?;
        } else {
            return Err(StorageMigrationError::UnsafeEntry(path));
        }
    }
    if entries != directory_entries(directory, relative)? {
        return Err(StorageMigrationError::TreeChanged(relative.to_path_buf()));
    }
    fingerprint.record(relative, object_identity(directory)?)?;
    Ok(())
}

#[cfg(target_os = "linux")]
#[derive(Debug, Eq, PartialEq)]
struct TreeFingerprint {
    entries: u64,
    path_bytes: u64,
    digest: [u8; 32],
}

#[cfg(target_os = "linux")]
struct TreeFingerprintBuilder {
    entries: u64,
    path_bytes: u64,
    digest: Sha256,
}

#[cfg(target_os = "linux")]
impl TreeFingerprintBuilder {
    fn new() -> Self {
        Self {
            entries: 0,
            path_bytes: 0,
            digest: Sha256::new(),
        }
    }

    fn record(
        &mut self,
        path: &Path,
        identity: ObjectIdentity,
    ) -> Result<(), StorageMigrationError> {
        record_migration_object(&mut self.entries, &mut self.path_bytes, path)?;
        let path = path.as_os_str().as_bytes();
        self.digest.update((path.len() as u64).to_be_bytes());
        self.digest.update(path);
        self.digest.update(identity.mount.to_be_bytes());
        self.digest.update(identity.device.to_be_bytes());
        self.digest.update(identity.inode.to_be_bytes());
        self.digest.update(identity.file_type.to_be_bytes());
        self.digest.update(identity.links.to_be_bytes());
        Ok(())
    }

    fn finish(self) -> TreeFingerprint {
        TreeFingerprint {
            entries: self.entries,
            path_bytes: self.path_bytes,
            digest: self.digest.finalize().into(),
        }
    }
}

#[cfg(target_os = "linux")]
struct MigrationBudget {
    entries: u64,
    path_bytes: u64,
}

#[cfg(target_os = "linux")]
impl MigrationBudget {
    const fn new() -> Self {
        Self {
            entries: 0,
            path_bytes: 0,
        }
    }

    fn record(&mut self, path: &Path) -> Result<(), StorageMigrationError> {
        record_migration_object(&mut self.entries, &mut self.path_bytes, path)
    }
}

#[cfg(target_os = "linux")]
fn record_migration_object(
    entries: &mut u64,
    path_bytes: &mut u64,
    path: &Path,
) -> Result<(), StorageMigrationError> {
    let next_entries = entries.checked_add(1);
    let next_path_bytes = path_bytes.checked_add(path.as_os_str().as_bytes().len() as u64);
    match (next_entries, next_path_bytes) {
        (Some(next_entries), Some(next_path_bytes))
            if next_entries <= MAXIMUM_MIGRATION_ENTRIES
                && next_path_bytes <= MAXIMUM_MIGRATION_PATH_BYTES =>
        {
            *entries = next_entries;
            *path_bytes = next_path_bytes;
            Ok(())
        }
        _ => Err(StorageMigrationError::MigrationLimitExceeded(
            path.to_path_buf(),
        )),
    }
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ObjectIdentity {
    mount: u64,
    device: u64,
    inode: u64,
    file_type: u32,
    links: u64,
}

#[cfg(target_os = "linux")]
fn object_identity(file: &File) -> Result<ObjectIdentity, StorageMigrationError> {
    let metadata = file.metadata().map_err(StorageMigrationError::Io)?;
    Ok(ObjectIdentity {
        mount: linux_mount_id(file)?,
        device: metadata.dev(),
        inode: metadata.ino(),
        file_type: metadata.mode() & nix::libc::S_IFMT,
        links: metadata.nlink(),
    })
}

#[cfg(target_os = "linux")]
fn linux_mount_id(file: &File) -> Result<u64, StorageMigrationError> {
    let mut bytes = Vec::with_capacity(MAXIMUM_FD_INFO_BYTES as usize);
    File::open(format!("/proc/self/fdinfo/{}", file.as_raw_fd()))
        .and_then(|input| {
            input
                .take(MAXIMUM_FD_INFO_BYTES + 1)
                .read_to_end(&mut bytes)
        })
        .map_err(StorageMigrationError::Io)?;
    if bytes.len() as u64 > MAXIMUM_FD_INFO_BYTES {
        return Err(StorageMigrationError::InvalidMountInformation);
    }
    let contents =
        std::str::from_utf8(&bytes).map_err(|_| StorageMigrationError::InvalidMountInformation)?;
    contents
        .lines()
        .find_map(|line| line.strip_prefix("mnt_id:").map(str::trim))
        .ok_or(StorageMigrationError::InvalidMountInformation)?
        .parse()
        .map_err(|_| StorageMigrationError::InvalidMountInformation)
}

#[cfg(target_os = "linux")]
fn set_identity_and_mode(
    file: &File,
    owner: Option<Uid>,
    group: Gid,
    mode: Mode,
) -> Result<(), StorageMigrationError> {
    let metadata = file.metadata().map_err(StorageMigrationError::Io)?;
    let owner = owner.filter(|owner| metadata.uid() != owner.as_raw());
    let group = (metadata.gid() != group.as_raw()).then_some(group);
    if owner.is_some() || group.is_some() {
        fchown(file, owner, group).map_err(nix_io_error)?;
    }
    fchmod(file, mode).map_err(nix_io_error)
}

#[cfg(target_os = "linux")]
fn nix_io_error(error: nix::errno::Errno) -> StorageMigrationError {
    StorageMigrationError::Io(io::Error::from_raw_os_error(error as i32))
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
pub(crate) enum StorageMigrationError {
    RootRequired,
    InvalidRoot(PathBuf),
    OverlappingRoots,
    UnknownIdentity(std::ffi::OsString),
    QueueBusy,
    DirectoryNotEmpty(PathBuf),
    PosixAcl(PathBuf),
    UnsafeEntry(PathBuf),
    TreeChanged(PathBuf),
    MigrationLimitExceeded(PathBuf),
    Attestation(PathAttestationError),
    InvalidMountInformation,
    Io(io::Error),
}

#[cfg(target_os = "linux")]
impl fmt::Display for StorageMigrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RootRequired => formatter.write_str("storage migration must run as root"),
            Self::InvalidRoot(path) => write!(
                formatter,
                "storage migration root must be an existing canonical absolute directory: {}",
                path.display()
            ),
            Self::OverlappingRoots => {
                formatter.write_str("storage migration roots must not overlap")
            }
            Self::UnknownIdentity(identity) => write!(
                formatter,
                "storage migration identity does not exist: {}",
                identity.to_string_lossy()
            ),
            Self::QueueBusy => {
                formatter.write_str("stop every Gateway and Worker before migrating storage")
            }
            Self::DirectoryNotEmpty(path) => write!(
                formatter,
                "storage migration requires an empty directory: {}",
                path.display()
            ),
            Self::PosixAcl(path) => write!(
                formatter,
                "storage migration rejected a POSIX ACL: {}",
                path.display()
            ),
            Self::UnsafeEntry(path) => write!(
                formatter,
                "storage migration rejected a link, special file, hard link, cross-mount entry, or excessive depth: {}",
                path.display()
            ),
            Self::TreeChanged(path) => write!(
                formatter,
                "storage migration tree changed while it was being processed: {}",
                path.display()
            ),
            Self::MigrationLimitExceeded(path) => write!(
                formatter,
                "storage migration exceeds the supported limit of {MAXIMUM_MIGRATION_ENTRIES} objects or {MAXIMUM_MIGRATION_PATH_BYTES} relative-path bytes near: {}",
                path.display()
            ),
            Self::Attestation(error) => {
                write!(
                    formatter,
                    "storage migration root attestation failed: {error}"
                )
            }
            Self::InvalidMountInformation => {
                formatter.write_str("storage migration could not verify a mount identity")
            }
            Self::Io(error) => write!(formatter, "storage migration I/O failed: {error}"),
        }
    }
}

#[cfg(target_os = "linux")]
impl std::error::Error for StorageMigrationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Attestation(error) => Some(error),
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub(crate) enum AdminStatusError {
    Config(WorkerConfigError),
    Inspect(OperationalStatusError),
    DeadlineOverflow,
    Json(serde_json::Error),
    Io(io::Error),
}

impl fmt::Display for AdminStatusError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(error) => write!(formatter, "invalid Worker configuration: {error}"),
            Self::Inspect(error) => write!(formatter, "operational status failed: {error}"),
            Self::DeadlineOverflow => formatter.write_str("operational status deadline overflowed"),
            Self::Json(error) => write!(formatter, "operational status JSON failed: {error}"),
            Self::Io(error) => write!(formatter, "operational status output failed: {error}"),
        }
    }
}

impl std::error::Error for AdminStatusError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Config(error) => Some(error),
            Self::Inspect(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::Io(error) => Some(error),
            Self::DeadlineOverflow => None,
        }
    }
}

#[derive(Debug)]
pub(crate) enum AdminRetentionError {
    Config(WorkerConfigError),
    Retain(ReleaseMaintenanceError),
    DeadlineOverflow,
    Json(serde_json::Error),
    Io(io::Error),
}

impl fmt::Display for AdminRetentionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(error) => write!(formatter, "invalid Worker configuration: {error}"),
            Self::Retain(error) => write!(formatter, "release maintenance failed: {error}"),
            Self::DeadlineOverflow => {
                formatter.write_str("release maintenance deadline overflowed")
            }
            Self::Json(error) => write!(formatter, "release maintenance JSON failed: {error}"),
            Self::Io(error) => write!(formatter, "release maintenance output failed: {error}"),
        }
    }
}

impl std::error::Error for AdminRetentionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Config(error) => Some(error),
            Self::Retain(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::Io(error) => Some(error),
            Self::DeadlineOverflow => None,
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod migration_tests {
    use std::fs;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use agent_knowledge_queue::{FileQueue, PackagePolicy};
    use nix::unistd::{Gid, Uid};

    use super::{
        MAXIMUM_MIGRATION_ENTRIES, MAXIMUM_MIGRATION_PATH_BYTES, MigrationBudget,
        StorageMigrationError, migrate_v1_storage_with_ids, worker_owned_queue_file,
    };

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn worker_queue_metadata_is_limited_to_exact_sidecar_paths() {
        assert!(worker_owned_queue_file(Path::new(
            "processing/01K00000000000000000000000/phase.json"
        )));
        assert!(worker_owned_queue_file(Path::new(
            "failed/01K00000000000000000000000/result.json"
        )));
        assert!(worker_owned_queue_file(Path::new(
            "worker-tmp/.phase-01K00000000000000000000000"
        )));
        assert!(!worker_owned_queue_file(Path::new("phase.json")));
        assert!(!worker_owned_queue_file(Path::new(
            "pending/01K00000000000000000000000/payload/phase.json"
        )));
        assert!(!worker_owned_queue_file(Path::new(
            "pending/not-a-request/result.json"
        )));
        assert!(!worker_owned_queue_file(Path::new("worker-tmp/unexpected")));
    }

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn create() -> Self {
            let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "agent-knowledge-migration-test-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir(&path)
                .unwrap_or_else(|error| panic!("migration test root must be created: {error}"));
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
                panic!("migration test root must be removed: {error}");
            }
        }
    }

    #[test]
    fn migration_budget_rejects_each_supported_bound() {
        let mut entry_budget = MigrationBudget {
            entries: MAXIMUM_MIGRATION_ENTRIES,
            path_bytes: 0,
        };
        assert!(matches!(
            entry_budget.record(Path::new("fictional-entry")),
            Err(StorageMigrationError::MigrationLimitExceeded(_))
        ));

        let mut path_budget = MigrationBudget {
            entries: 0,
            path_bytes: MAXIMUM_MIGRATION_PATH_BYTES,
        };
        assert!(matches!(
            path_budget.record(Path::new("x")),
            Err(StorageMigrationError::MigrationLimitExceeded(_))
        ));
    }

    #[test]
    fn migrates_populated_roots_through_pinned_descriptors() {
        let fixture = TestDirectory::create();
        let queue = fixture.path().join("queue");
        let git = fixture.path().join("repository");
        let content = fixture.path().join("content");
        drop(
            FileQueue::initialize(&queue, PackagePolicy::default())
                .unwrap_or_else(|error| panic!("queue fixture must initialize: {error}")),
        );
        fs::create_dir(&git)
            .unwrap_or_else(|error| panic!("repository fixture must be created: {error}"));
        fs::create_dir(&content)
            .unwrap_or_else(|error| panic!("content fixture must be created: {error}"));
        fs::create_dir_all(content.join("projects/fictional-project"))
            .unwrap_or_else(|error| panic!("content descendants must be created: {error}"));
        fs::write(
            content.join("projects/fictional-project/index.md"),
            b"Fictional migration fixture.\n",
        )
        .unwrap_or_else(|error| panic!("content fixture must be written: {error}"));
        fs::write(git.join("HEAD"), b"ref: refs/heads/main\n")
            .unwrap_or_else(|error| panic!("repository fixture must be written: {error}"));

        let mut output = Vec::new();
        migrate_v1_storage_with_ids(
            &queue,
            &git,
            &content,
            Uid::effective(),
            Gid::current(),
            Gid::current(),
            &mut output,
        )
        .unwrap_or_else(|error| panic!("storage migration fixture must succeed: {error}"));

        assert_eq!(output, b"{\"status\":\"completed\"}\n");
        assert_eq!(
            fs::metadata(&queue)
                .unwrap_or_else(|error| panic!("queue metadata must be readable: {error}"))
                .permissions()
                .mode()
                & 0o7777,
            0o2770
        );
        let content_file = fs::metadata(content.join("projects/fictional-project/index.md"))
            .unwrap_or_else(|error| panic!("migrated content metadata must be readable: {error}"));
        assert_eq!(content_file.gid(), Gid::current().as_raw());
        assert_eq!(content_file.permissions().mode() & 0o7777, 0o640);
    }
}

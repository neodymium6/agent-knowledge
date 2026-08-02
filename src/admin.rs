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
use agent_knowledge_core::{PathAttestation, PathAttestationError};

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
const MAXIMUM_MIGRATION_DEPTH: usize = 128;
#[cfg(target_os = "linux")]
const MAXIMUM_FD_INFO_BYTES: u64 = 16 * 1024;

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

    let mut queue_fingerprint = TreeFingerprintBuilder::new();
    migrate_directory(
        &queue.file,
        queue_group,
        Mode::from_bits_truncate(0o2770),
        Mode::from_bits_truncate(0o660),
        0,
        Path::new(""),
        &mut queue_fingerprint,
    )?;
    let queue_fingerprint = queue_fingerprint.finish();
    let mut git_fingerprint = TreeFingerprintBuilder::new();
    migrate_directory(
        &git.file,
        gateway_group,
        Mode::from_bits_truncate(0o2750),
        Mode::from_bits_truncate(0o640),
        0,
        Path::new(""),
        &mut git_fingerprint,
    )?;
    let git_fingerprint = git_fingerprint.finish();
    let mut content_fingerprint = TreeFingerprintBuilder::new();
    migrate_directory(
        &content.file,
        gateway_group,
        Mode::from_bits_truncate(0o2750),
        Mode::from_bits_truncate(0o640),
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
fn resolve_user(value: &OsStr) -> Result<Uid, StorageMigrationError> {
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
fn resolve_group(value: &OsStr) -> Result<Gid, StorageMigrationError> {
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
        OFlag::O_RDWR | OFlag::O_NONBLOCK | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
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
    if directory_entries(&directory)?.is_empty() {
        Ok(())
    } else {
        Err(StorageMigrationError::DirectoryNotEmpty(path.to_path_buf()))
    }
}

#[cfg(target_os = "linux")]
fn migrate_directory(
    directory: &File,
    group: Gid,
    directory_mode: Mode,
    file_mode: Mode,
    depth: usize,
    relative: &Path,
    fingerprint: &mut TreeFingerprintBuilder,
) -> Result<(), StorageMigrationError> {
    if depth > MAXIMUM_MIGRATION_DEPTH {
        return Err(StorageMigrationError::UnsafeEntry(relative.to_path_buf()));
    }
    let entries = directory_entries(directory)?;
    for name in &entries {
        let path = relative.join(OsStr::from_bytes(name.to_bytes()));
        let pinned = open_child_path(directory, name)?;
        let identity = object_identity(&pinned)?;
        if identity.file_type == nix::libc::S_IFDIR {
            let child = open_child_directory(directory, name)?;
            if object_identity(&child)? != identity {
                return Err(StorageMigrationError::TreeChanged(path));
            }
            migrate_directory(
                &child,
                group,
                directory_mode,
                file_mode,
                depth + 1,
                &path,
                fingerprint,
            )?;
        } else if identity.file_type == nix::libc::S_IFREG && identity.links == 1 {
            let child = open_child_regular(directory, name)?;
            if object_identity(&child)? != identity {
                return Err(StorageMigrationError::TreeChanged(path));
            }
            set_regular_identity_and_mode(&child, group, file_mode, &path)?;
            fingerprint.record(&path, object_identity(&child)?)?;
        } else {
            return Err(StorageMigrationError::UnsafeEntry(path));
        }
    }
    if entries != directory_entries(directory)? {
        return Err(StorageMigrationError::TreeChanged(relative.to_path_buf()));
    }
    set_identity_and_mode(directory, None, group, directory_mode)?;
    fingerprint.record(relative, object_identity(directory)?)?;
    Ok(())
}

#[cfg(target_os = "linux")]
fn directory_entries(directory: &File) -> Result<Vec<CString>, StorageMigrationError> {
    let cloned = directory.try_clone().map_err(StorageMigrationError::Io)?;
    let mut entries = Dir::from_fd(cloned.into()).map_err(nix_io_error)?;
    let mut names = Vec::new();
    for entry in entries.iter() {
        let entry = entry.map_err(nix_io_error)?;
        let name = entry.file_name().to_bytes();
        if name != b"." && name != b".." {
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
        OFlag::O_RDWR | OFlag::O_NONBLOCK | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
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
    group: Gid,
    mode: Mode,
    path: &Path,
) -> Result<(), StorageMigrationError> {
    require_regular(file, path)?;
    set_identity_and_mode(file, None, group, mode)?;
    require_regular(file, path).map(|_| ())
}

#[cfg(target_os = "linux")]
fn verify_fingerprint(
    root: &File,
    expected: &TreeFingerprint,
) -> Result<(), StorageMigrationError> {
    let mut observed = TreeFingerprintBuilder::new();
    fingerprint_directory(root, 0, Path::new(""), &mut observed)?;
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
) -> Result<(), StorageMigrationError> {
    if depth > MAXIMUM_MIGRATION_DEPTH {
        return Err(StorageMigrationError::UnsafeEntry(relative.to_path_buf()));
    }
    let entries = directory_entries(directory)?;
    for name in &entries {
        let path = relative.join(OsStr::from_bytes(name.to_bytes()));
        let pinned = open_child_path(directory, name)?;
        let identity = object_identity(&pinned)?;
        if identity.file_type == nix::libc::S_IFDIR {
            let child = open_child_directory(directory, name)?;
            if object_identity(&child)? != identity {
                return Err(StorageMigrationError::TreeChanged(path));
            }
            fingerprint_directory(&child, depth + 1, &path, fingerprint)?;
        } else if identity.file_type != nix::libc::S_IFREG || identity.links != 1 {
            return Err(StorageMigrationError::UnsafeEntry(path));
        } else {
            fingerprint.record(&path, identity)?;
        }
    }
    if entries != directory_entries(directory)? {
        return Err(StorageMigrationError::TreeChanged(relative.to_path_buf()));
    }
    fingerprint.record(relative, object_identity(directory)?)?;
    Ok(())
}

#[cfg(target_os = "linux")]
#[derive(Debug, Eq, PartialEq)]
struct TreeFingerprint {
    entries: u64,
    digest: [u8; 32],
}

#[cfg(target_os = "linux")]
struct TreeFingerprintBuilder {
    entries: u64,
    digest: Sha256,
}

#[cfg(target_os = "linux")]
impl TreeFingerprintBuilder {
    fn new() -> Self {
        Self {
            entries: 0,
            digest: Sha256::new(),
        }
    }

    fn record(
        &mut self,
        path: &Path,
        identity: ObjectIdentity,
    ) -> Result<(), StorageMigrationError> {
        self.entries = self
            .entries
            .checked_add(1)
            .ok_or_else(|| StorageMigrationError::UnsafeEntry(path.to_path_buf()))?;
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
            digest: self.digest.finalize().into(),
        }
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
    UnsafeEntry(PathBuf),
    TreeChanged(PathBuf),
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

    use super::migrate_v1_storage_with_ids;

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

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

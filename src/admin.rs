use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
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
use nix::dir::Dir;
#[cfg(target_os = "linux")]
use nix::fcntl::{OFlag, OpenHow, ResolveFlag, openat2};
#[cfg(target_os = "linux")]
use nix::sys::stat::{Mode, fchmod};
#[cfg(target_os = "linux")]
use nix::unistd::{Gid, Group, Uid, User, fchown};

#[cfg(target_os = "linux")]
const MAXIMUM_MIGRATION_DEPTH: usize = 128;

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
    let queue_path = canonical_root(queue_root)?;
    let git_path = canonical_root(git_directory)?;
    let content_path = canonical_root(content_root)?;
    ensure_disjoint_roots([&queue_path, &git_path, &content_path])?;
    let queue = open_root(&queue_path)?;
    let git = open_root(&git_path)?;
    let content = open_root(&content_path)?;

    let queue_lock = open_regular_beneath(&queue, Path::new(".locks/queue.lock"))?;
    let worker_lock = open_regular_beneath(&queue, Path::new(".locks/repository-writer.lock"))?;
    queue_lock
        .try_lock()
        .map_err(|_| StorageMigrationError::QueueBusy)?;
    worker_lock
        .try_lock()
        .map_err(|_| StorageMigrationError::QueueBusy)?;
    require_empty_directory(&queue, Path::new("incoming"))?;
    require_empty_directory(&queue, Path::new("quarantine"))?;

    migrate_directory(
        &queue,
        queue_group,
        Mode::from_bits_truncate(0o2770),
        Mode::from_bits_truncate(0o660),
        0,
        Path::new(""),
    )?;
    migrate_directory(
        &git,
        gateway_group,
        Mode::from_bits_truncate(0o2750),
        Mode::from_bits_truncate(0o640),
        0,
        Path::new(""),
    )?;
    migrate_directory(
        &content,
        gateway_group,
        Mode::from_bits_truncate(0o2750),
        Mode::from_bits_truncate(0o640),
        0,
        Path::new(""),
    )?;

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
            queue.try_clone().map_err(StorageMigrationError::Io)?
        } else {
            open_directory_beneath(&queue, Path::new(relative))?
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
    queue.sync_all().map_err(StorageMigrationError::Io)?;
    git.sync_all().map_err(StorageMigrationError::Io)?;
    content.sync_all().map_err(StorageMigrationError::Io)?;
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
fn canonical_root(path: &Path) -> Result<PathBuf, StorageMigrationError> {
    if !path.is_absolute() || path == Path::new("/") {
        return Err(StorageMigrationError::InvalidRoot(path.to_path_buf()));
    }
    let canonical = fs::canonicalize(path).map_err(StorageMigrationError::Io)?;
    if canonical != path {
        return Err(StorageMigrationError::InvalidRoot(path.to_path_buf()));
    }
    Ok(canonical)
}

#[cfg(target_os = "linux")]
fn ensure_disjoint_roots(roots: [&Path; 3]) -> Result<(), StorageMigrationError> {
    for (index, root) in roots.iter().enumerate() {
        if roots
            .iter()
            .enumerate()
            .any(|(other_index, other)| index != other_index && root.starts_with(other))
        {
            return Err(StorageMigrationError::OverlappingRoots);
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn open_root(path: &Path) -> Result<File, StorageMigrationError> {
    OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC)
        .open(path)
        .map_err(StorageMigrationError::Io)
}

#[cfg(target_os = "linux")]
fn open_directory_beneath(root: &File, path: &Path) -> Result<File, StorageMigrationError> {
    let how = OpenHow::new()
        .flags(OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC)
        .resolve(
            ResolveFlag::RESOLVE_BENEATH
                | ResolveFlag::RESOLVE_NO_SYMLINKS
                | ResolveFlag::RESOLVE_NO_XDEV,
        );
    openat2(root, path, how)
        .map(File::from)
        .map_err(nix_io_error)
}

#[cfg(target_os = "linux")]
fn open_regular_beneath(root: &File, path: &Path) -> Result<File, StorageMigrationError> {
    let how = OpenHow::new()
        .flags(OFlag::O_RDWR | OFlag::O_NONBLOCK | OFlag::O_CLOEXEC)
        .resolve(
            ResolveFlag::RESOLVE_BENEATH
                | ResolveFlag::RESOLVE_NO_SYMLINKS
                | ResolveFlag::RESOLVE_NO_XDEV,
        );
    let file = openat2(root, path, how)
        .map(File::from)
        .map_err(nix_io_error)?;
    require_regular(&file, path)?;
    Ok(file)
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
) -> Result<(), StorageMigrationError> {
    if depth > MAXIMUM_MIGRATION_DEPTH {
        return Err(StorageMigrationError::UnsafeEntry(relative.to_path_buf()));
    }
    let entries = directory_entries(directory)?;
    for name in &entries {
        let path = relative.join(OsStr::from_bytes(name.to_bytes()));
        match open_child_directory(directory, name) {
            Ok(child) => {
                migrate_directory(&child, group, directory_mode, file_mode, depth + 1, &path)?
            }
            Err(nix::errno::Errno::ENOTDIR) => {
                let child = open_child_file(directory, name).map_err(nix_io_error)?;
                require_regular(&child, &path)?;
                set_identity_and_mode(&child, None, group, file_mode)?;
            }
            Err(error) => return Err(nix_io_error(error)),
        }
    }
    if entries != directory_entries(directory)? {
        return Err(StorageMigrationError::TreeChanged(relative.to_path_buf()));
    }
    set_identity_and_mode(directory, None, group, directory_mode)
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
fn open_child_directory(parent: &File, name: &CStr) -> Result<File, nix::errno::Errno> {
    let how = OpenHow::new()
        .flags(OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC)
        .resolve(ResolveFlag::RESOLVE_NO_SYMLINKS | ResolveFlag::RESOLVE_NO_XDEV);
    openat2(parent, name, how).map(File::from)
}

#[cfg(target_os = "linux")]
fn open_child_file(parent: &File, name: &CStr) -> Result<File, nix::errno::Errno> {
    let how = OpenHow::new()
        .flags(OFlag::O_RDWR | OFlag::O_NONBLOCK | OFlag::O_CLOEXEC)
        .resolve(ResolveFlag::RESOLVE_NO_SYMLINKS | ResolveFlag::RESOLVE_NO_XDEV);
    openat2(parent, name, how).map(File::from)
}

#[cfg(target_os = "linux")]
fn require_regular(file: &File, path: &Path) -> Result<(), StorageMigrationError> {
    let metadata = file.metadata().map_err(StorageMigrationError::Io)?;
    if metadata.is_file() && metadata.nlink() == 1 {
        Ok(())
    } else {
        Err(StorageMigrationError::UnsafeEntry(path.to_path_buf()))
    }
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
            Self::Io(error) => write!(formatter, "storage migration I/O failed: {error}"),
        }
    }
}

#[cfg(target_os = "linux")]
impl std::error::Error for StorageMigrationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
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

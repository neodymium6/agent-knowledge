use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use agent_knowledge_core::BatchId;
use serde::{Deserialize, Serialize};
use time::{OffsetDateTime, UtcOffset};
use ulid::Ulid;

const BY_ID_DIRECTORY: &str = "by-id";
const STAGING_DIRECTORY: &str = ".staging";
const CURRENT_ENTRY: &str = "current";
const BINDING_FILE: &str = ".release-store-binding-v1";
const MANIFEST_FILE: &str = ".agent-knowledge-release.json";
const MANIFEST_SCHEMA_VERSION: u16 = 1;
const MAXIMUM_MANIFEST_BYTES: u64 = 16 * 1024;

/// Bounds applied to generated Quartz output before it can be published.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReleasePolicy {
    pub maximum_entries: u64,
    pub maximum_file_bytes: u64,
    pub maximum_total_bytes: u64,
}

impl Default for ReleasePolicy {
    fn default() -> Self {
        Self {
            maximum_entries: 100_000,
            maximum_file_bytes: 64 * 1024 * 1024,
            maximum_total_bytes: 1024 * 1024 * 1024,
        }
    }
}

impl ReleasePolicy {
    fn validate(self) -> Result<Self, ReleaseError> {
        if self.maximum_entries == 0
            || self.maximum_file_bytes == 0
            || self.maximum_total_bytes == 0
            || self.maximum_file_bytes > self.maximum_total_bytes
        {
            Err(ReleaseError::InvalidPolicy)
        } else {
            Ok(self)
        }
    }
}

/// An immutable release ready for activation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedRelease {
    release_id: String,
    commit: String,
}

impl PreparedRelease {
    #[must_use]
    pub fn release_id(&self) -> &str {
        &self.release_id
    }

    #[must_use]
    pub fn commit(&self) -> &str {
        &self.commit
    }
}

/// The release currently selected by `releases/current`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActiveRelease {
    release_id: String,
    commit: String,
}

impl ActiveRelease {
    #[must_use]
    pub fn release_id(&self) -> &str {
        &self.release_id
    }

    #[must_use]
    pub fn commit(&self) -> &str {
        &self.commit
    }
}

/// Batch-scoped Quartz output directory retaining its pinned parent lease.
#[derive(Clone, Debug)]
pub struct BuildDirectory {
    path: PathBuf,
    _staging_lease: Arc<File>,
}

impl BuildDirectory {
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Pinned release roots with atomic staging and activation.
#[derive(Clone, Debug)]
pub struct ReleaseStore {
    configured_root: PathBuf,
    root: PathBuf,
    root_handle: Arc<File>,
    by_id: PinnedDirectory,
    staging: PinnedDirectory,
    policy: ReleasePolicy,
}

#[derive(Clone, Debug)]
struct PinnedDirectory {
    configured: PathBuf,
    stable: PathBuf,
    handle: Arc<File>,
}

impl ReleaseStore {
    /// Creates fixed release directories and binds them to this storage root.
    pub fn open(root: impl AsRef<Path>, policy: ReleasePolicy) -> Result<Self, ReleaseError> {
        let policy = policy.validate()?;
        ensure_or_create_directory(root.as_ref())?;
        let configured_root = fs::canonicalize(root).map_err(ReleaseError::Io)?;
        let root_handle = Arc::new(File::open(&configured_root).map_err(ReleaseError::Io)?);
        let root = stable_directory_path(&root_handle, &configured_root)?;
        ensure_or_create_directory(&root.join(BY_ID_DIRECTORY))?;
        ensure_or_create_directory(&root.join(STAGING_DIRECTORY))?;
        sync_directory(&root)?;
        let by_id = pin_directory(
            configured_root.join(BY_ID_DIRECTORY),
            root.join(BY_ID_DIRECTORY),
        )?;
        let staging = pin_directory(
            configured_root.join(STAGING_DIRECTORY),
            root.join(STAGING_DIRECTORY),
        )?;
        validate_common_mount(&root_handle, [&by_id, &staging])?;
        let store = Self {
            configured_root,
            root,
            root_handle,
            by_id,
            staging,
            policy,
        };
        store.ensure_binding()?;
        store.validate_live_storage()?;
        store.active_release()?;
        Ok(store)
    }

    /// Creates an empty, batch-scoped directory for one Quartz build.
    pub fn begin_build(&self, batch_id: BatchId) -> Result<BuildDirectory, ReleaseError> {
        self.validate_live_storage()?;
        let path = self.staging.stable.join(batch_id.to_string());
        match fs::create_dir(&path) {
            Ok(()) => {
                sync_directory(&self.staging.stable)?;
                Ok(BuildDirectory {
                    path,
                    _staging_lease: Arc::clone(&self.staging.handle),
                })
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                Err(ReleaseError::BuildAlreadyExists(batch_id))
            }
            Err(error) => Err(ReleaseError::Io(error)),
        }
    }

    /// Removes only the derived staging output for one canonical batch ID.
    pub fn discard_build(&self, batch_id: BatchId) -> Result<(), ReleaseError> {
        self.validate_live_storage()?;
        let path = self.staging.stable.join(batch_id.to_string());
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_dir() => {
                fs::remove_dir_all(path).map_err(ReleaseError::Io)?;
                sync_directory(&self.staging.stable)?;
                self.validate_live_storage()
            }
            Ok(_) => Err(ReleaseError::InvalidDirectory(path)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(ReleaseError::Io(error)),
        }
    }

    /// Validates and durably promotes generated output into `by-id/`.
    pub fn prepare(
        &self,
        batch_id: BatchId,
        commit: &str,
        created_at: OffsetDateTime,
    ) -> Result<PreparedRelease, ReleaseError> {
        self.validate_live_storage()?;
        validate_commit(commit)?;
        let release_id = release_id(created_at, commit);
        let staging = self.staging.stable.join(batch_id.to_string());
        let destination = self.by_id.stable.join(&release_id);
        if path_exists(&destination)? {
            validate_release_tree(&destination, self.policy)?;
            validate_manifest(&destination, &release_id, commit)?;
            sync_tree(&destination)?;
            sync_directory(&self.by_id.stable)?;
            return Ok(PreparedRelease {
                release_id,
                commit: commit.into(),
            });
        }
        ensure_real_directory(&staging)?;
        let manifest = ReleaseManifest {
            schema_version: MANIFEST_SCHEMA_VERSION,
            release_id: release_id.clone(),
            commit: commit.into(),
            created_at: created_at.to_offset(UtcOffset::UTC),
        };
        validate_release_tree(&staging, self.policy)?;
        ensure_manifest(&staging.join(MANIFEST_FILE), &manifest)?;
        sync_tree(&staging)?;
        fs::rename(&staging, &destination).map_err(ReleaseError::Io)?;
        sync_directory(&self.staging.stable)?;
        sync_directory(&self.by_id.stable)?;
        self.validate_live_storage()?;
        validate_manifest(&destination, &release_id, commit)?;
        Ok(PreparedRelease {
            release_id,
            commit: commit.into(),
        })
    }

    /// Atomically changes `current` to an already prepared immutable release.
    pub fn activate(&self, release: &PreparedRelease) -> Result<ActiveRelease, ReleaseError> {
        self.validate_live_storage()?;
        let release_path = self.by_id.stable.join(&release.release_id);
        validate_manifest(&release_path, &release.release_id, &release.commit)?;
        let target = PathBuf::from(BY_ID_DIRECTORY).join(&release.release_id);
        let current = self.root.join(CURRENT_ENTRY);
        if let Some(active) = self.active_release()?
            && active.release_id == release.release_id
            && active.commit == release.commit
        {
            return Ok(active);
        }
        let temporary = self.root.join(format!(".current-{}", Ulid::generate()));
        create_symlink(&target, &temporary)?;
        if let Err(error) = fs::rename(&temporary, &current) {
            let _ = fs::remove_file(&temporary);
            return Err(ReleaseError::Io(error));
        }
        sync_directory(&self.root)?;
        self.validate_live_storage()?;
        self.active_release()?
            .ok_or(ReleaseError::InvalidCurrentEntry)
    }

    /// Returns the validated active release, if publication has not started.
    pub fn active_release(&self) -> Result<Option<ActiveRelease>, ReleaseError> {
        self.validate_live_storage()?;
        let current = self.root.join(CURRENT_ENTRY);
        let metadata = match fs::symlink_metadata(&current) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(ReleaseError::Io(error)),
        };
        if !metadata.file_type().is_symlink() {
            return Err(ReleaseError::InvalidCurrentEntry);
        }
        let target = fs::read_link(&current).map_err(ReleaseError::Io)?;
        let mut components = target.components();
        if components
            .next()
            .and_then(|value| value.as_os_str().to_str())
            != Some(BY_ID_DIRECTORY)
        {
            return Err(ReleaseError::InvalidCurrentEntry);
        }
        let release_id = components
            .next()
            .and_then(|value| value.as_os_str().to_str())
            .filter(|_| components.next().is_none())
            .ok_or(ReleaseError::InvalidCurrentEntry)?;
        let manifest = read_manifest(&self.by_id.stable.join(release_id))?;
        if manifest.release_id != release_id {
            return Err(ReleaseError::InvalidManifest);
        }
        validate_commit(&manifest.commit)?;
        Ok(Some(ActiveRelease {
            release_id: manifest.release_id,
            commit: manifest.commit,
        }))
    }

    fn ensure_binding(&self) -> Result<(), ReleaseError> {
        let expected = binding_bytes(
            &self.configured_root,
            &self.root_handle,
            [&self.by_id, &self.staging],
        )?;
        let path = self.root.join(BINDING_FILE);
        match fs::symlink_metadata(&path) {
            Ok(_) => validate_binding(&path, &expected),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let mut file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path)
                    .map_err(ReleaseError::Io)?;
                file.write_all(&expected).map_err(ReleaseError::Io)?;
                file.sync_all().map_err(ReleaseError::Io)?;
                sync_directory(&self.root)
            }
            Err(error) => Err(ReleaseError::Io(error)),
        }
    }

    fn validate_live_storage(&self) -> Result<(), ReleaseError> {
        let canonical = fs::canonicalize(&self.configured_root).map_err(ReleaseError::Io)?;
        if canonical != self.configured_root {
            return Err(ReleaseError::StorageBindingMismatch);
        }
        validate_pinned_directory(&self.configured_root, &self.root_handle)?;
        validate_pinned_directory(&self.by_id.configured, &self.by_id.handle)?;
        validate_pinned_directory(&self.staging.configured, &self.staging.handle)?;
        let expected = binding_bytes(
            &self.configured_root,
            &self.root_handle,
            [&self.by_id, &self.staging],
        )?;
        validate_binding(&self.root.join(BINDING_FILE), &expected)
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReleaseManifest {
    schema_version: u16,
    release_id: String,
    commit: String,
    #[serde(with = "time::serde::rfc3339")]
    created_at: OffsetDateTime,
}

fn release_id(created_at: OffsetDateTime, commit: &str) -> String {
    let created_at = created_at.to_offset(UtcOffset::UTC);
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z-{commit}",
        created_at.year(),
        u8::from(created_at.month()),
        created_at.day(),
        created_at.hour(),
        created_at.minute(),
        created_at.second()
    )
}

fn validate_commit(commit: &str) -> Result<(), ReleaseError> {
    if matches!(commit.len(), 40 | 64)
        && commit
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(ReleaseError::InvalidCommit)
    }
}

fn write_manifest(path: &Path, manifest: &ReleaseManifest) -> Result<(), ReleaseError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(ReleaseError::Io)?;
    serde_json::to_writer(&mut file, manifest).map_err(ReleaseError::ManifestEncoding)?;
    file.write_all(b"\n").map_err(ReleaseError::Io)?;
    file.sync_all().map_err(ReleaseError::Io)
}

fn ensure_manifest(path: &Path, manifest: &ReleaseManifest) -> Result<(), ReleaseError> {
    match fs::symlink_metadata(path) {
        Ok(_) => {
            let release = path.parent().ok_or(ReleaseError::InvalidManifest)?;
            validate_manifest(release, &manifest.release_id, &manifest.commit)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => write_manifest(path, manifest),
        Err(error) => Err(ReleaseError::Io(error)),
    }
}

fn read_manifest(release: &Path) -> Result<ReleaseManifest, ReleaseError> {
    ensure_real_directory(release)?;
    let path = release.join(MANIFEST_FILE);
    let metadata = fs::symlink_metadata(&path).map_err(ReleaseError::Io)?;
    if !metadata.file_type().is_file() || metadata.len() > MAXIMUM_MANIFEST_BYTES {
        return Err(ReleaseError::InvalidManifest);
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)
        .and_then(|file| {
            file.take(MAXIMUM_MANIFEST_BYTES + 1)
                .read_to_end(&mut bytes)
        })
        .map_err(ReleaseError::Io)?;
    if bytes.len() as u64 > MAXIMUM_MANIFEST_BYTES {
        return Err(ReleaseError::InvalidManifest);
    }
    let manifest: ReleaseManifest =
        serde_json::from_slice(&bytes).map_err(ReleaseError::ManifestDecoding)?;
    if manifest.schema_version != MANIFEST_SCHEMA_VERSION {
        return Err(ReleaseError::InvalidManifest);
    }
    Ok(manifest)
}

fn validate_manifest(release: &Path, release_id: &str, commit: &str) -> Result<(), ReleaseError> {
    let manifest = read_manifest(release)?;
    if manifest.release_id == release_id && manifest.commit == commit {
        Ok(())
    } else {
        Err(ReleaseError::InvalidManifest)
    }
}

fn validate_release_tree(root: &Path, policy: ReleasePolicy) -> Result<(), ReleaseError> {
    let mut entries = 0_u64;
    let mut bytes = 0_u64;
    let mut site_files = 0_u64;
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory).map_err(ReleaseError::Io)? {
            let entry = entry.map_err(ReleaseError::Io)?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path).map_err(ReleaseError::Io)?;
            entries = entries.checked_add(1).ok_or(ReleaseError::OutputTooLarge)?;
            if entries > policy.maximum_entries {
                return Err(ReleaseError::OutputTooLarge);
            }
            if metadata.file_type().is_dir() {
                pending.push(path);
            } else if metadata.file_type().is_file() {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::MetadataExt;
                    if metadata.nlink() != 1 {
                        return Err(ReleaseError::UnsafeOutput(path));
                    }
                }
                if path.file_name().and_then(|name| name.to_str()) != Some(MANIFEST_FILE) {
                    if metadata.len() > policy.maximum_file_bytes {
                        return Err(ReleaseError::OutputTooLarge);
                    }
                    bytes = bytes
                        .checked_add(metadata.len())
                        .ok_or(ReleaseError::OutputTooLarge)?;
                    if bytes > policy.maximum_total_bytes {
                        return Err(ReleaseError::OutputTooLarge);
                    }
                    site_files += 1;
                }
            } else {
                return Err(ReleaseError::UnsafeOutput(path));
            }
        }
    }
    if site_files == 0 {
        Err(ReleaseError::EmptyOutput)
    } else {
        Ok(())
    }
}

fn sync_tree(path: &Path) -> Result<(), ReleaseError> {
    for entry in fs::read_dir(path).map_err(ReleaseError::Io)? {
        let entry = entry.map_err(ReleaseError::Io)?;
        let metadata = fs::symlink_metadata(entry.path()).map_err(ReleaseError::Io)?;
        if metadata.file_type().is_dir() {
            sync_tree(&entry.path())?;
        } else if metadata.file_type().is_file() {
            File::open(entry.path())
                .and_then(|file| file.sync_all())
                .map_err(ReleaseError::Io)?;
        } else {
            return Err(ReleaseError::UnsafeOutput(entry.path()));
        }
    }
    sync_directory(path)
}

fn pin_directory(configured: PathBuf, stable: PathBuf) -> Result<PinnedDirectory, ReleaseError> {
    ensure_directory_target(&stable)?;
    Ok(PinnedDirectory {
        handle: Arc::new(File::open(&stable).map_err(ReleaseError::Io)?),
        configured,
        stable,
    })
}

fn stable_directory_path(handle: &File, configured: &Path) -> Result<PathBuf, ReleaseError> {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let _ = configured;
        let path = PathBuf::from(format!(
            "/proc/{}/fd/{}",
            std::process::id(),
            handle.as_raw_fd()
        ));
        ensure_directory_target(&path)?;
        Ok(path)
    }
    #[cfg(not(target_os = "linux"))]
    {
        Ok(configured.to_path_buf())
    }
}

fn ensure_or_create_directory(path: &Path) -> Result<(), ReleaseError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(()),
        Ok(_) => Err(ReleaseError::InvalidDirectory(path.into())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir(path).map_err(ReleaseError::Io)?;
            let parent = path
                .parent()
                .ok_or_else(|| ReleaseError::InvalidDirectory(path.into()))?;
            sync_directory(parent)
        }
        Err(error) => Err(ReleaseError::Io(error)),
    }
}

fn ensure_real_directory(path: &Path) -> Result<(), ReleaseError> {
    let metadata = fs::symlink_metadata(path).map_err(ReleaseError::Io)?;
    if metadata.file_type().is_dir() {
        Ok(())
    } else {
        Err(ReleaseError::InvalidDirectory(path.into()))
    }
}

fn ensure_directory_target(path: &Path) -> Result<(), ReleaseError> {
    let metadata = fs::metadata(path).map_err(ReleaseError::Io)?;
    if metadata.is_dir() {
        Ok(())
    } else {
        Err(ReleaseError::InvalidDirectory(path.into()))
    }
}

fn validate_pinned_directory(path: &Path, pinned: &File) -> Result<(), ReleaseError> {
    let configured = fs::symlink_metadata(path).map_err(ReleaseError::Io)?;
    let pinned_metadata = pinned.metadata().map_err(ReleaseError::Io)?;
    if !configured.file_type().is_dir() || !same_metadata(&configured, &pinned_metadata) {
        return Err(ReleaseError::StorageBindingMismatch);
    }
    #[cfg(target_os = "linux")]
    {
        let live = File::open(path).map_err(ReleaseError::Io)?;
        if !same_metadata(
            &live.metadata().map_err(ReleaseError::Io)?,
            &pinned_metadata,
        ) || mount_id(&live)? != mount_id(pinned)?
        {
            return Err(ReleaseError::StorageBindingMismatch);
        }
    }
    Ok(())
}

#[cfg(unix)]
fn same_metadata(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(not(unix))]
fn same_metadata(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.file_type() == right.file_type() && left.len() == right.len()
}

fn binding_bytes<'a>(
    root: &Path,
    root_handle: &File,
    directories: impl IntoIterator<Item = &'a PinnedDirectory>,
) -> Result<Vec<u8>, ReleaseError> {
    let mut bytes = b"agent-knowledge-release-store-v1\0".to_vec();
    bytes.extend_from_slice(root.as_os_str().as_encoded_bytes());
    for handle in std::iter::once(root_handle).chain(
        directories
            .into_iter()
            .map(|directory| directory.handle.as_ref()),
    ) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let metadata = handle.metadata().map_err(ReleaseError::Io)?;
            bytes.push(0);
            bytes.extend_from_slice(&metadata.dev().to_le_bytes());
            bytes.extend_from_slice(&metadata.ino().to_le_bytes());
        }
    }
    Ok(bytes)
}

fn validate_binding(path: &Path, expected: &[u8]) -> Result<(), ReleaseError> {
    let metadata = fs::symlink_metadata(path).map_err(ReleaseError::Io)?;
    if !metadata.file_type().is_file() || metadata.len() > 64 * 1024 {
        return Err(ReleaseError::StorageBindingMismatch);
    }
    let mut actual = Vec::with_capacity(metadata.len() as usize);
    File::open(path)
        .and_then(|file| file.take(64 * 1024 + 1).read_to_end(&mut actual))
        .map_err(ReleaseError::Io)?;
    if actual == expected {
        Ok(())
    } else {
        Err(ReleaseError::StorageBindingMismatch)
    }
}

#[cfg(target_os = "linux")]
fn validate_common_mount<'a>(
    root: &File,
    directories: impl IntoIterator<Item = &'a PinnedDirectory>,
) -> Result<(), ReleaseError> {
    let expected = mount_id(root)?;
    for directory in directories {
        if mount_id(&directory.handle)? != expected {
            return Err(ReleaseError::CrossMountStorage);
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn validate_common_mount<'a>(
    _root: &File,
    _directories: impl IntoIterator<Item = &'a PinnedDirectory>,
) -> Result<(), ReleaseError> {
    Ok(())
}

#[cfg(target_os = "linux")]
fn mount_id(file: &File) -> Result<u64, ReleaseError> {
    use std::os::fd::AsRawFd;
    const MAXIMUM_FDINFO_BYTES: u64 = 16 * 1024;
    let mut bytes = Vec::with_capacity(MAXIMUM_FDINFO_BYTES as usize);
    File::open(format!("/proc/self/fdinfo/{}", file.as_raw_fd()))
        .and_then(|file| file.take(MAXIMUM_FDINFO_BYTES + 1).read_to_end(&mut bytes))
        .map_err(ReleaseError::Io)?;
    if bytes.len() as u64 > MAXIMUM_FDINFO_BYTES {
        return Err(ReleaseError::InvalidMountMetadata);
    }
    let contents = std::str::from_utf8(&bytes).map_err(|_| ReleaseError::InvalidMountMetadata)?;
    contents
        .lines()
        .find_map(|line| line.strip_prefix("mnt_id:").map(str::trim))
        .ok_or(ReleaseError::InvalidMountMetadata)?
        .parse()
        .map_err(|_| ReleaseError::InvalidMountMetadata)
}

fn path_exists(path: &Path) -> Result<bool, ReleaseError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(ReleaseError::Io(error)),
    }
}

fn sync_directory(path: &Path) -> Result<(), ReleaseError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(ReleaseError::Io)
}

#[cfg(unix)]
fn create_symlink(target: &Path, link: &Path) -> Result<(), ReleaseError> {
    std::os::unix::fs::symlink(target, link).map_err(ReleaseError::Io)
}

#[cfg(not(unix))]
fn create_symlink(_target: &Path, _link: &Path) -> Result<(), ReleaseError> {
    Err(ReleaseError::SymlinkUnsupported)
}

#[derive(Debug)]
pub enum ReleaseError {
    InvalidPolicy,
    InvalidDirectory(PathBuf),
    InvalidCommit,
    InvalidManifest,
    InvalidCurrentEntry,
    InvalidMountMetadata,
    CrossMountStorage,
    StorageBindingMismatch,
    BuildAlreadyExists(BatchId),
    OutputTooLarge,
    EmptyOutput,
    UnsafeOutput(PathBuf),
    SymlinkUnsupported,
    ManifestEncoding(serde_json::Error),
    ManifestDecoding(serde_json::Error),
    Io(io::Error),
}

impl fmt::Display for ReleaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPolicy => formatter.write_str("release limits are invalid"),
            Self::InvalidDirectory(path) => {
                write!(
                    formatter,
                    "release path is not a real directory: {}",
                    path.display()
                )
            }
            Self::InvalidCommit => formatter.write_str("release commit is invalid"),
            Self::InvalidManifest => formatter.write_str("release manifest is invalid"),
            Self::InvalidCurrentEntry => formatter.write_str("current release entry is invalid"),
            Self::InvalidMountMetadata => formatter.write_str("mount metadata is invalid"),
            Self::CrossMountStorage => formatter.write_str("release directories cross mounts"),
            Self::StorageBindingMismatch => formatter.write_str("release storage binding changed"),
            Self::BuildAlreadyExists(batch_id) => {
                write!(
                    formatter,
                    "release build already exists for batch {batch_id}"
                )
            }
            Self::OutputTooLarge => formatter.write_str("release output exceeds configured limits"),
            Self::EmptyOutput => formatter.write_str("release output contains no site files"),
            Self::UnsafeOutput(path) => {
                write!(
                    formatter,
                    "release output contains an unsafe entry: {}",
                    path.display()
                )
            }
            Self::SymlinkUnsupported => {
                formatter.write_str("atomic release activation requires symbolic links")
            }
            Self::ManifestEncoding(error) => write!(formatter, "manifest encoding failed: {error}"),
            Self::ManifestDecoding(error) => write!(formatter, "manifest decoding failed: {error}"),
            Self::Io(error) => write!(formatter, "release storage I/O failed: {error}"),
        }
    }
}

impl std::error::Error for ReleaseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ManifestEncoding(error) | Self::ManifestDecoding(error) => Some(error),
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests;

use std::fmt;
use std::fs::{self, File, TryLockError};
use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use agent_knowledge_core::{
    PathAttestation, PathAttestationError, PinnedDirectory, PinnedPathError,
};
use ulid::Ulid;

use super::{TantivySearchError, TantivySearchIndex};
use crate::{CommittedSnapshot, SearchMetadataFields};

const BY_ID_DIRECTORY: &str = "by-id";
const STAGING_DIRECTORY: &str = ".staging";
const CURRENT_ENTRY: &str = "current";

/// An immutable persistent search index ready for selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedSearchIndex {
    generation_id: String,
    commit: String,
}

impl PreparedSearchIndex {
    /// Returns the unique immutable generation identifier.
    #[must_use]
    pub fn generation_id(&self) -> &str {
        &self.generation_id
    }

    /// Returns the exact Git commit represented by the index.
    #[must_use]
    pub fn commit(&self) -> &str {
        &self.commit
    }
}

/// The validated search index selected by `current`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActiveSearchIndex {
    generation_id: String,
    commit: String,
}

impl ActiveSearchIndex {
    /// Returns the unique immutable generation identifier.
    #[must_use]
    pub fn generation_id(&self) -> &str {
        &self.generation_id
    }

    /// Returns the exact Git commit represented by the index.
    #[must_use]
    pub fn commit(&self) -> &str {
        &self.commit
    }

    fn prepared(&self) -> PreparedSearchIndex {
        PreparedSearchIndex {
            generation_id: self.generation_id.clone(),
            commit: self.commit.clone(),
        }
    }
}

/// Pinned derived-index storage with immutable generations and atomic selection.
#[derive(Clone, Debug)]
pub struct SearchIndexStore {
    configured_root: PathBuf,
    root: PinnedStoreDirectory,
    by_id: PinnedStoreDirectory,
    staging: PinnedStoreDirectory,
    mutation_available: Arc<AtomicBool>,
}

#[derive(Clone, Debug)]
struct PinnedStoreDirectory {
    configured: PathBuf,
    stable: PathBuf,
    handle: Arc<File>,
}

impl SearchIndexStore {
    /// Opens the immutable index selected by `current` without creating or
    /// modifying search-index storage.
    ///
    /// The selector must be the exact relative `by-id/<generation>` shape, so
    /// a malformed link cannot redirect a reader outside the configured root.
    ///
    /// # Errors
    ///
    /// Returns an error when the existing store layout or selected generation
    /// is missing, malformed, incompatible, or inconsistent.
    pub fn open_active_read_only(
        root: impl AsRef<Path>,
    ) -> Result<Option<TantivySearchIndex>, SearchIndexStoreError> {
        Self::open_active_read_only_with(root.as_ref(), || {})
    }

    fn open_active_read_only_with(
        root: &Path,
        after_root_pin: impl FnOnce(),
    ) -> Result<Option<TantivySearchIndex>, SearchIndexStoreError> {
        let root = pin_directory(root)?;
        after_root_pin();
        validate_pinned_directory(&root)?;
        let current = root.stable.join(CURRENT_ENTRY);
        let metadata = match fs::symlink_metadata(&current) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(SearchIndexStoreError::Io(error)),
        };
        if !metadata.file_type().is_symlink() {
            return Err(SearchIndexStoreError::InvalidCurrentEntry);
        }
        let target = fs::read_link(&current).map_err(SearchIndexStoreError::Io)?;
        let generation_id = generation_from_target(&target)?;
        let by_id = pin_active_directory(&root.stable.join(BY_ID_DIRECTORY))?;
        let generation = pin_active_directory(&by_id.stable.join(generation_id))?;
        let index = TantivySearchIndex::open_pinned_directory(
            &generation.stable,
            Arc::clone(&generation.handle),
        )
        .map_err(SearchIndexStoreError::search)?;
        validate_pinned_directory(&root)?;
        validate_pinned_directory(&by_id)?;
        validate_pinned_directory(&generation)?;
        validate_generation_id(generation_id, index.commit())?;
        Ok(Some(index))
    }

    #[cfg(test)]
    pub(crate) fn open_active_read_only_after_root_pin(
        root: impl AsRef<Path>,
        after_root_pin: impl FnOnce(),
    ) -> Result<Option<TantivySearchIndex>, SearchIndexStoreError> {
        Self::open_active_read_only_with(root.as_ref(), after_root_pin)
    }

    /// Creates or opens the fixed derived-index storage layout.
    ///
    /// Existing immutable generations are validated only when selected. An
    /// interrupted unique staging generation is never selected and may remain
    /// for a later bounded retention pass.
    ///
    /// # Errors
    ///
    /// Returns an error when storage cannot be created and pinned, fixed
    /// directories overlap mount boundaries, or `current` is malformed.
    pub fn open(root: impl AsRef<Path>) -> Result<Self, SearchIndexStoreError> {
        let store = Self::open_layout(root.as_ref())?;
        store.active_index()?;
        Ok(store)
    }

    /// Opens the store and quarantines a corrupt active derived generation.
    ///
    /// Storage identity, fixed-directory, and mount-boundary failures remain
    /// fatal. Only the active selection and its selected immutable generation
    /// are quarantined, allowing a caller to rebuild them from canonical
    /// content.
    ///
    /// # Errors
    ///
    /// Returns an error when storage is unsafe, another writer owns it, the
    /// active failure is not recoverable derived-data corruption, or durable
    /// quarantine fails.
    pub fn open_recovering(root: impl AsRef<Path>) -> Result<Self, SearchIndexStoreError> {
        let store = Self::open_layout(root.as_ref())?;
        match store.active_index() {
            Ok(_) => Ok(store),
            Err(error) if recoverable_active_error(&error) => {
                store.quarantine_active()?;
                store.active_index()?;
                Ok(store)
            }
            Err(error) => Err(error),
        }
    }

    fn open_layout(root: &Path) -> Result<Self, SearchIndexStoreError> {
        ensure_or_create_directory(root)?;
        let configured_root = fs::canonicalize(root).map_err(SearchIndexStoreError::Io)?;
        let root = pin_directory(&configured_root)?;
        ensure_or_create_directory(&root.stable.join(BY_ID_DIRECTORY))?;
        ensure_or_create_directory(&root.stable.join(STAGING_DIRECTORY))?;
        sync_directory(&root.stable)?;
        let by_id = pin_directory(&configured_root.join(BY_ID_DIRECTORY))?;
        let staging = pin_directory(&configured_root.join(STAGING_DIRECTORY))?;
        validate_common_filesystem(&root, &by_id, &staging)?;
        let store = Self {
            configured_root,
            root,
            by_id,
            staging,
            mutation_available: Arc::new(AtomicBool::new(true)),
        };
        store.validate_live_storage()?;
        Ok(store)
    }

    /// Attests the root selected and pinned while opening the store.
    ///
    /// # Errors
    ///
    /// Returns an error if the configured root no longer names the pinned
    /// object or its mount ancestry cannot be inspected.
    pub fn storage_attestation(&self) -> Result<PathAttestation, PathAttestationError> {
        PathAttestation::capture(&self.configured_root, &self.root.handle)
    }

    /// Builds and durably promotes a unique immutable generation.
    ///
    /// If `current` already represents the snapshot commit, its validated
    /// generation is reused. Otherwise the new generation is fully committed
    /// before it becomes eligible for [`Self::activate`].
    ///
    /// # Errors
    ///
    /// Returns an error when storage changes, another writer owns the store,
    /// snapshot indexing fails, or the generation cannot be promoted.
    pub fn prepare(
        &self,
        snapshot: &CommittedSnapshot,
        metadata_fields: SearchMetadataFields,
    ) -> Result<PreparedSearchIndex, SearchIndexStoreError> {
        let _mutation = self.lock_mutation()?;
        self.validate_live_storage()?;
        if let Some(active) = self.active_index()?
            && active.commit == snapshot.commit()
        {
            return Ok(active.prepared());
        }

        let generation_id = generation_id(snapshot.commit());
        let staging = self.staging.stable.join(&generation_id);
        let destination = self.by_id.stable.join(&generation_id);
        let built = TantivySearchIndex::build_in_directory(snapshot, metadata_fields, &staging)
            .map_err(SearchIndexStoreError::search)?;
        if built.commit() != snapshot.commit() {
            return Err(SearchIndexStoreError::GenerationCommitMismatch);
        }
        drop(built);
        sync_directory(&self.staging.stable)?;
        fs::rename(&staging, &destination).map_err(SearchIndexStoreError::Io)?;
        sync_directory(&self.by_id.stable)?;
        sync_directory(&self.staging.stable)?;
        let reopened = TantivySearchIndex::open_directory(&destination)
            .map_err(SearchIndexStoreError::search)?;
        if reopened.commit() != snapshot.commit() {
            return Err(SearchIndexStoreError::GenerationCommitMismatch);
        }
        drop(reopened);
        self.validate_live_storage()?;
        Ok(PreparedSearchIndex {
            generation_id,
            commit: snapshot.commit().to_owned(),
        })
    }

    /// Atomically changes `current` to one validated immutable generation.
    ///
    /// # Errors
    ///
    /// Returns an error when storage changes, the prepared generation is
    /// invalid, or the atomic symlink replacement cannot be made durable.
    pub fn activate(
        &self,
        prepared: &PreparedSearchIndex,
    ) -> Result<ActiveSearchIndex, SearchIndexStoreError> {
        let _mutation = self.lock_mutation()?;
        self.validate_live_storage()?;
        validate_generation_id(&prepared.generation_id, &prepared.commit)?;
        let index =
            TantivySearchIndex::open_directory(self.by_id.stable.join(&prepared.generation_id))
                .map_err(SearchIndexStoreError::search)?;
        if index.commit() != prepared.commit {
            return Err(SearchIndexStoreError::GenerationCommitMismatch);
        }
        drop(index);
        if let Some(active) = self.active_index()?
            && active.generation_id == prepared.generation_id
            && active.commit == prepared.commit
        {
            sync_directory(&self.root.stable)?;
            return Ok(active);
        }

        let target = PathBuf::from(BY_ID_DIRECTORY).join(&prepared.generation_id);
        let current = self.root.stable.join(CURRENT_ENTRY);
        let temporary = self
            .root
            .stable
            .join(format!(".current-{}", Ulid::generate()));
        create_symlink(&target, &temporary)?;
        if let Err(error) = fs::rename(&temporary, &current) {
            let _ = fs::remove_file(&temporary);
            return Err(SearchIndexStoreError::Io(error));
        }
        sync_directory(&self.root.stable)?;
        self.validate_live_storage()?;
        let active = self
            .active_index()?
            .ok_or(SearchIndexStoreError::InvalidCurrentEntry)?;
        if active.generation_id != prepared.generation_id || active.commit != prepared.commit {
            return Err(SearchIndexStoreError::ActivationConflict);
        }
        Ok(active)
    }

    /// Returns the validated generation selected by `current`, if one exists.
    ///
    /// # Errors
    ///
    /// Returns an error when storage changed or `current` does not select one
    /// valid immutable generation below `by-id`.
    pub fn active_index(&self) -> Result<Option<ActiveSearchIndex>, SearchIndexStoreError> {
        self.validate_live_storage()?;
        let current = self.root.stable.join(CURRENT_ENTRY);
        let metadata = match fs::symlink_metadata(&current) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(SearchIndexStoreError::Io(error)),
        };
        if !metadata.file_type().is_symlink() {
            return Err(SearchIndexStoreError::InvalidCurrentEntry);
        }
        let target = fs::read_link(&current).map_err(SearchIndexStoreError::Io)?;
        let generation_id = generation_from_target(&target)?;
        let index = TantivySearchIndex::open_directory(self.by_id.stable.join(generation_id))
            .map_err(SearchIndexStoreError::search)?;
        let commit = index.commit().to_owned();
        validate_generation_id(generation_id, &commit)?;
        Ok(Some(ActiveSearchIndex {
            generation_id: generation_id.to_owned(),
            commit,
        }))
    }

    fn validate_live_storage(&self) -> Result<(), SearchIndexStoreError> {
        validate_pinned_directory(&self.root)?;
        validate_pinned_directory(&self.by_id)?;
        validate_pinned_directory(&self.staging)?;
        validate_common_filesystem(&self.root, &self.by_id, &self.staging)
    }

    fn quarantine_active(&self) -> Result<(), SearchIndexStoreError> {
        let _mutation = self.lock_mutation()?;
        self.validate_live_storage()?;
        match self.active_index() {
            Ok(_) => return Ok(()),
            Err(error) if recoverable_active_error(&error) => {}
            Err(error) => return Err(error),
        }
        let current = self.root.stable.join(CURRENT_ENTRY);
        let metadata = match fs::symlink_metadata(&current) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(SearchIndexStoreError::Io(error)),
        };
        let selected_generation = if metadata.file_type().is_symlink() {
            fs::read_link(&current)
                .ok()
                .and_then(|target| generation_from_target(&target).ok().map(str::to_owned))
        } else {
            None
        };
        let quarantine_id = Ulid::generate();
        let quarantined_current = self
            .staging
            .stable
            .join(format!("invalid-current-{quarantine_id}"));
        fs::rename(&current, &quarantined_current).map_err(SearchIndexStoreError::Io)?;
        sync_directory(&self.root.stable)?;
        sync_directory(&self.staging.stable)?;

        if let Some(generation) = selected_generation {
            let selected = self.by_id.stable.join(&generation);
            match fs::symlink_metadata(&selected) {
                Ok(_) => {
                    let quarantined_generation = self
                        .staging
                        .stable
                        .join(format!("invalid-generation-{generation}-{quarantine_id}"));
                    fs::rename(&selected, &quarantined_generation)
                        .map_err(SearchIndexStoreError::Io)?;
                    sync_directory(&self.by_id.stable)?;
                    sync_directory(&self.staging.stable)?;
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(SearchIndexStoreError::Io(error)),
            }
        }
        self.validate_live_storage()
    }

    fn lock_mutation(&self) -> Result<MutationLease<'_>, SearchIndexStoreError> {
        if self
            .mutation_available
            .compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return Err(SearchIndexStoreError::Busy);
        }
        let root_lock = match File::open(&self.root.stable) {
            Ok(root) => root,
            Err(error) => {
                self.mutation_available.store(true, Ordering::Release);
                return Err(SearchIndexStoreError::Io(error));
            }
        };
        match root_lock.try_lock() {
            Ok(()) => Ok(MutationLease {
                available: &self.mutation_available,
                root_lock,
            }),
            Err(TryLockError::WouldBlock) => {
                self.mutation_available.store(true, Ordering::Release);
                Err(SearchIndexStoreError::Busy)
            }
            Err(TryLockError::Error(error)) => {
                self.mutation_available.store(true, Ordering::Release);
                Err(SearchIndexStoreError::Io(error))
            }
        }
    }
}

struct MutationLease<'a> {
    available: &'a AtomicBool,
    root_lock: File,
}

impl Drop for MutationLease<'_> {
    fn drop(&mut self) {
        let _ = self.root_lock.unlock();
        self.available.store(true, Ordering::Release);
    }
}

fn generation_id(commit: &str) -> String {
    format!("{commit}-{}", Ulid::generate())
}

fn recoverable_active_error(error: &SearchIndexStoreError) -> bool {
    matches!(
        error,
        SearchIndexStoreError::Search(_)
            | SearchIndexStoreError::InvalidGeneration
            | SearchIndexStoreError::GenerationCommitMismatch
            | SearchIndexStoreError::InvalidCurrentEntry
    )
}

fn validate_generation_id(generation_id: &str, commit: &str) -> Result<(), SearchIndexStoreError> {
    let Some(suffix) = generation_id
        .strip_prefix(commit)
        .and_then(|rest| rest.strip_prefix('-'))
    else {
        return Err(SearchIndexStoreError::InvalidGeneration);
    };
    if suffix.len() != 26 || Ulid::from_string(suffix).is_err() {
        return Err(SearchIndexStoreError::InvalidGeneration);
    }
    Ok(())
}

fn generation_from_target(target: &Path) -> Result<&str, SearchIndexStoreError> {
    let mut components = target.components();
    if components.next() != Some(Component::Normal(BY_ID_DIRECTORY.as_ref())) {
        return Err(SearchIndexStoreError::InvalidCurrentEntry);
    }
    let generation = match components.next() {
        Some(Component::Normal(generation)) => generation
            .to_str()
            .ok_or(SearchIndexStoreError::InvalidCurrentEntry)?,
        _ => return Err(SearchIndexStoreError::InvalidCurrentEntry),
    };
    if components.next().is_some() {
        return Err(SearchIndexStoreError::InvalidCurrentEntry);
    }
    Ok(generation)
}

fn ensure_or_create_directory(path: &Path) -> Result<(), SearchIndexStoreError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(()),
        Ok(_) => Err(SearchIndexStoreError::InvalidStorage),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if let Err(error) = fs::create_dir(path)
                && error.kind() != io::ErrorKind::AlreadyExists
            {
                return Err(SearchIndexStoreError::Io(error));
            }
            let metadata = fs::symlink_metadata(path).map_err(SearchIndexStoreError::Io)?;
            if !metadata.file_type().is_dir() {
                return Err(SearchIndexStoreError::InvalidStorage);
            }
            sync_parent(path)
        }
        Err(error) => Err(SearchIndexStoreError::Io(error)),
    }
}

fn pin_directory(path: &Path) -> Result<PinnedStoreDirectory, SearchIndexStoreError> {
    let pinned = PinnedDirectory::open(path).map_err(SearchIndexStoreError::pinned)?;
    let handle = Arc::new(
        pinned
            .try_clone_file()
            .map_err(SearchIndexStoreError::pinned)?,
    );
    let configured = fs::canonicalize(path).map_err(SearchIndexStoreError::Io)?;
    let stable = stable_directory_path(&handle, &configured)?;
    let directory = PinnedStoreDirectory {
        configured,
        stable,
        handle,
    };
    validate_pinned_directory(&directory)?;
    Ok(directory)
}

fn pin_active_directory(path: &Path) -> Result<PinnedStoreDirectory, SearchIndexStoreError> {
    pin_directory(path).map_err(|_| SearchIndexStoreError::InvalidCurrentEntry)
}

fn validate_pinned_directory(
    directory: &PinnedStoreDirectory,
) -> Result<(), SearchIndexStoreError> {
    PathAttestation::capture(&directory.configured, &directory.handle)
        .map(|_| ())
        .map_err(SearchIndexStoreError::attestation)
}

fn stable_directory_path(
    handle: &File,
    _configured: &Path,
) -> Result<PathBuf, SearchIndexStoreError> {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;

        let stable = PathBuf::from(format!(
            "/proc/{}/fd/{}",
            std::process::id(),
            handle.as_raw_fd()
        ));
        fs::metadata(&stable).map_err(SearchIndexStoreError::Io)?;
        Ok(stable)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = handle;
        Ok(_configured.to_path_buf())
    }
}

fn validate_common_filesystem(
    root: &PinnedStoreDirectory,
    by_id: &PinnedStoreDirectory,
    staging: &PinnedStoreDirectory,
) -> Result<(), SearchIndexStoreError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        let device = root
            .handle
            .metadata()
            .map_err(SearchIndexStoreError::Io)?
            .dev();
        for directory in [by_id, staging] {
            if directory
                .handle
                .metadata()
                .map_err(SearchIndexStoreError::Io)?
                .dev()
                != device
            {
                return Err(SearchIndexStoreError::CrossFilesystemStorage);
            }
        }
    }
    Ok(())
}

fn sync_parent(path: &Path) -> Result<(), SearchIndexStoreError> {
    let parent = path.parent().ok_or(SearchIndexStoreError::InvalidStorage)?;
    sync_directory(parent)
}

fn sync_directory(path: &Path) -> Result<(), SearchIndexStoreError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(SearchIndexStoreError::Io)
}

#[cfg(unix)]
fn create_symlink(target: &Path, link: &Path) -> Result<(), SearchIndexStoreError> {
    std::os::unix::fs::symlink(target, link).map_err(SearchIndexStoreError::Io)
}

#[cfg(not(unix))]
fn create_symlink(_target: &Path, _link: &Path) -> Result<(), SearchIndexStoreError> {
    Err(SearchIndexStoreError::UnsupportedPlatform)
}

/// Failure while preparing, selecting, or validating derived search indexes.
#[derive(Debug)]
pub enum SearchIndexStoreError {
    /// Filesystem I/O failed.
    Io(io::Error),
    /// A pinned path operation failed.
    Pinned(PinnedPathError),
    /// A pinned storage identity or mount binding changed.
    Attestation(PathAttestationError),
    /// Tantivy index construction or validation failed.
    Search(Box<TantivySearchError>),
    /// Another writer owns this search index store.
    Busy,
    /// A fixed storage path was not a real directory.
    InvalidStorage,
    /// Staging and immutable generations are on different filesystems.
    CrossFilesystemStorage,
    /// A generation identifier was malformed or disagreed with its index.
    InvalidGeneration,
    /// The index payload disagreed with the expected committed snapshot.
    GenerationCommitMismatch,
    /// `current` was not the expected relative generation symlink.
    InvalidCurrentEntry,
    /// Selection completed but did not resolve to the requested generation.
    ActivationConflict,
    /// Atomic symbolic-link replacement is unavailable on this platform.
    UnsupportedPlatform,
}

impl SearchIndexStoreError {
    fn pinned(error: PinnedPathError) -> Self {
        Self::Pinned(error)
    }

    fn attestation(error: PathAttestationError) -> Self {
        Self::Attestation(error)
    }

    fn search(error: TantivySearchError) -> Self {
        Self::Search(Box::new(error))
    }
}

impl fmt::Display for SearchIndexStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "search index storage I/O failed: {error}"),
            Self::Pinned(error) => write!(formatter, "search index storage pin failed: {error}"),
            Self::Attestation(error) => {
                write!(formatter, "search index storage binding changed: {error}")
            }
            Self::Search(error) => write!(formatter, "search index validation failed: {error}"),
            Self::Busy => formatter.write_str("search index storage is busy"),
            Self::InvalidStorage => formatter.write_str("search index storage layout is invalid"),
            Self::CrossFilesystemStorage => {
                formatter.write_str("search index storage spans multiple filesystems")
            }
            Self::InvalidGeneration => {
                formatter.write_str("search index generation identifier is invalid")
            }
            Self::GenerationCommitMismatch => {
                formatter.write_str("search index generation commit is inconsistent")
            }
            Self::InvalidCurrentEntry => {
                formatter.write_str("search index current entry is invalid")
            }
            Self::ActivationConflict => {
                formatter.write_str("search index activation did not select its generation")
            }
            Self::UnsupportedPlatform => {
                formatter.write_str("search index activation is unsupported on this platform")
            }
        }
    }
}

impl std::error::Error for SearchIndexStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Pinned(error) => Some(error),
            Self::Attestation(error) => Some(error),
            Self::Search(error) => Some(error),
            _ => None,
        }
    }
}

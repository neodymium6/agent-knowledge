use std::fmt;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use agent_knowledge_core::{BatchId, Revision};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::{OffsetDateTime, UtcOffset};
use ulid::Ulid;

const BY_ID_DIRECTORY: &str = "by-id";
const BY_COMMIT_DIRECTORY: &str = "by-commit";
const BY_BATCH_DIRECTORY: &str = "by-batch";
const STAGING_DIRECTORY: &str = ".staging";
const SITE_DIRECTORY: &str = "site";
const CURRENT_ENTRY: &str = "current";
const BINDING_FILE: &str = ".release-store-binding-v3";
const MANIFEST_FILE: &str = ".agent-knowledge-release.json";
const MANIFEST_SCHEMA_VERSION: u16 = 2;
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
#[derive(Debug)]
pub struct BuildDirectory {
    batch_id: BatchId,
    configured: PathBuf,
    stable: PathBuf,
    path: PathBuf,
    batch_handle: Arc<File>,
    mutation_lease: MutationLease,
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
    by_commit: PinnedDirectory,
    by_batch: PinnedDirectory,
    staging: PinnedDirectory,
    policy: ReleasePolicy,
    mutation_available: Arc<AtomicBool>,
}

#[derive(Clone, Debug)]
struct PinnedDirectory {
    configured: PathBuf,
    stable: PathBuf,
    handle: Arc<File>,
}

struct BatchLease {
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
        ensure_or_create_directory(&root.join(BY_COMMIT_DIRECTORY))?;
        ensure_or_create_directory(&root.join(BY_BATCH_DIRECTORY))?;
        ensure_or_create_directory(&root.join(STAGING_DIRECTORY))?;
        sync_directory(&root)?;
        let by_id = pin_directory(
            configured_root.join(BY_ID_DIRECTORY),
            root.join(BY_ID_DIRECTORY),
        )?;
        let by_commit = pin_directory(
            configured_root.join(BY_COMMIT_DIRECTORY),
            root.join(BY_COMMIT_DIRECTORY),
        )?;
        let by_batch = pin_directory(
            configured_root.join(BY_BATCH_DIRECTORY),
            root.join(BY_BATCH_DIRECTORY),
        )?;
        let staging = pin_directory(
            configured_root.join(STAGING_DIRECTORY),
            root.join(STAGING_DIRECTORY),
        )?;
        validate_common_mount(&root_handle, [&by_id, &by_commit, &by_batch, &staging])?;
        let store = Self {
            configured_root,
            root,
            root_handle,
            by_id,
            by_commit,
            by_batch,
            staging,
            policy,
            mutation_available: Arc::new(AtomicBool::new(true)),
        };
        store.ensure_binding()?;
        store.validate_live_storage()?;
        store.active_release()?;
        Ok(store)
    }

    /// Creates an empty, batch-scoped directory for one Quartz build.
    pub fn begin_build(&self, batch_id: BatchId) -> Result<BuildDirectory, ReleaseError> {
        let mutation_lease = self.lock_mutation()?;
        self.validate_live_storage()?;
        let configured = self
            .configured_root
            .join(STAGING_DIRECTORY)
            .join(batch_id.to_string());
        let path = self.staging.stable.join(batch_id.to_string());
        match fs::create_dir(&path) {
            Ok(()) => {
                let handle = Arc::new(File::open(&path).map_err(ReleaseError::Io)?);
                lock_file(&handle)?;
                let stable = stable_directory_path(&handle, &configured)?;
                fs::create_dir(stable.join(SITE_DIRECTORY)).map_err(ReleaseError::Io)?;
                sync_directory(&stable)?;
                sync_directory(&self.staging.stable)?;
                Ok(BuildDirectory {
                    batch_id,
                    configured,
                    stable: stable.clone(),
                    path: stable.join(SITE_DIRECTORY),
                    batch_handle: handle,
                    mutation_lease,
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
        let _mutation = self.lock_mutation()?;
        self.validate_live_storage()?;
        if self.batch_intent(batch_id)?.is_some() {
            return Err(ReleaseError::BuildRecoveryRequired);
        }
        let configured = self
            .configured_root
            .join(STAGING_DIRECTORY)
            .join(batch_id.to_string());
        let path = self.staging.stable.join(batch_id.to_string());
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_dir() => {
                let batch = lock_batch_directory(&configured, &path)?;
                validate_pinned_directory(&batch.configured, &batch.handle)?;
                self.remove_batch_directory(batch_id, &batch)?;
                self.validate_live_storage()
            }
            Ok(_) => Err(ReleaseError::InvalidDirectory(path)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                sync_directory(&self.staging.stable)?;
                self.validate_live_storage()
            }
            Err(error) => Err(ReleaseError::Io(error)),
        }
    }

    /// Validates and durably promotes generated output into `by-id/`.
    pub fn prepare(
        &self,
        build: BuildDirectory,
        commit: &str,
        created_at: OffsetDateTime,
    ) -> Result<PreparedRelease, ReleaseError> {
        let BuildDirectory {
            batch_id,
            configured,
            stable,
            path: _,
            batch_handle,
            mutation_lease,
        } = build;
        mutation_lease.validate_root(&self.root_handle)?;
        self.validate_live_storage()?;
        validate_pinned_directory(&configured, &batch_handle)?;
        let batch = BatchLease {
            configured,
            stable,
            handle: batch_handle,
        };
        self.prepare_batch(batch_id, batch, commit, created_at)
    }

    /// Resumes an interrupted preparation from durable batch metadata.
    pub fn resume_prepare(
        &self,
        batch_id: BatchId,
        commit: &str,
    ) -> Result<PreparedRelease, ReleaseError> {
        let _mutation = self.lock_mutation()?;
        self.validate_live_storage()?;
        validate_commit(commit)?;
        let batch_configured = self
            .configured_root
            .join(STAGING_DIRECTORY)
            .join(batch_id.to_string());
        let batch_path = self.staging.stable.join(batch_id.to_string());
        let intent = self.batch_intent(batch_id)?;
        if intent.is_none() {
            if let Some(prepared) = self.prepared_for_commit(commit)? {
                let manifest = read_manifest(&self.by_id.stable.join(&prepared.release_id))?;
                if path_exists(&batch_path)? {
                    let batch = lock_batch_directory(&batch_configured, &batch_path)?;
                    self.cleanup_recovered_staging(batch_id, &batch, &manifest)?;
                } else {
                    sync_directory(&self.staging.stable)?;
                }
                self.remove_batch_intent(batch_id)?;
                return Ok(prepared);
            }
            if !path_exists(&batch_path)? {
                sync_directory(&self.staging.stable)?;
                return Err(ReleaseError::MissingRecoveryState);
            }
        }
        if let Some(release_id) = intent.as_deref() {
            let destination = self.by_id.stable.join(release_id);
            if path_exists(&destination)? {
                let manifest = read_manifest(&destination)?;
                if manifest.release_id != release_id || manifest.commit != commit {
                    return Err(ReleaseError::InvalidBatchIntent);
                }
                validate_release(&destination, &manifest, self.policy)?;
                sync_directory(&self.by_id.stable)?;
                self.ensure_commit_reference(&manifest)?;
                if path_exists(&batch_path)? {
                    let batch = lock_batch_directory(&batch_configured, &batch_path)?;
                    self.cleanup_recovered_staging(batch_id, &batch, &manifest)?;
                } else {
                    sync_directory(&self.staging.stable)?;
                }
                self.remove_batch_intent(batch_id)?;
                return Ok(PreparedRelease {
                    release_id: manifest.release_id,
                    commit: manifest.commit,
                });
            }
        }
        let batch = lock_batch_directory(&batch_configured, &batch_path)?;
        let manifest = read_manifest(&batch.stable.join(SITE_DIRECTORY))?;
        if manifest.commit != commit
            || intent
                .as_deref()
                .is_some_and(|release_id| release_id != manifest.release_id)
        {
            return Err(ReleaseError::InvalidBatchIntent);
        }
        self.prepare_batch(batch_id, batch, commit, manifest.created_at)
    }

    fn prepare_batch(
        &self,
        batch_id: BatchId,
        batch: BatchLease,
        commit: &str,
        created_at: OffsetDateTime,
    ) -> Result<PreparedRelease, ReleaseError> {
        validate_commit(commit)?;
        let release_id = release_id(created_at, commit);
        let destination = self.by_id.stable.join(&release_id);
        let created_at = created_at.to_offset(UtcOffset::UTC);
        if path_exists(&destination)? {
            let manifest = read_manifest(&destination)?;
            if manifest.release_id != release_id
                || manifest.commit != commit
                || manifest.created_at != created_at
            {
                return Err(ReleaseError::InvalidManifest);
            }
            validate_release(&destination, &manifest, self.policy)?;
            if validate_release_tree(&destination, self.policy, true)? != manifest.content_revision
            {
                return Err(ReleaseError::InvalidManifest);
            }
            sync_directory(&self.by_id.stable)?;
            self.ensure_commit_reference(&manifest)?;
            self.cleanup_recovered_staging(batch_id, &batch, &manifest)?;
            self.remove_batch_intent(batch_id)?;
            return Ok(PreparedRelease {
                release_id,
                commit: commit.into(),
            });
        }
        let staging = batch.stable.join(SITE_DIRECTORY);
        let content_revision = validate_release_tree(&staging, self.policy, false)?;
        let manifest = ReleaseManifest {
            schema_version: MANIFEST_SCHEMA_VERSION,
            release_id: release_id.clone(),
            commit: commit.into(),
            content_revision,
            created_at,
        };
        ensure_real_directory(&staging)?;
        ensure_manifest(&staging.join(MANIFEST_FILE), &manifest)?;
        if validate_release_tree(&staging, self.policy, true)? != manifest.content_revision {
            return Err(ReleaseError::OutputChanged);
        }
        self.ensure_batch_intent(batch_id, &manifest)?;
        fs::rename(&staging, &destination).map_err(ReleaseError::Io)?;
        // A recovered process must observe the destination before it may
        // observe the source removal as durable.
        sync_directory(&self.by_id.stable)?;
        self.ensure_commit_reference(&manifest)?;
        self.remove_batch_directory(batch_id, &batch)?;
        self.remove_batch_intent(batch_id)?;
        self.validate_live_storage()?;
        validate_release(&destination, &manifest, self.policy)?;
        Ok(PreparedRelease {
            release_id,
            commit: commit.into(),
        })
    }

    /// Atomically changes `current` to an already prepared immutable release.
    pub fn activate(&self, release: &PreparedRelease) -> Result<ActiveRelease, ReleaseError> {
        let _mutation = self.lock_mutation()?;
        self.validate_live_storage()?;
        let release_path = self.by_id.stable.join(&release.release_id);
        let manifest = read_manifest(&release_path)?;
        validate_release(&release_path, &manifest, self.policy)?;
        if manifest.release_id != release.release_id || manifest.commit != release.commit {
            return Err(ReleaseError::InvalidManifest);
        }
        let target = PathBuf::from(BY_ID_DIRECTORY).join(&release.release_id);
        let current = self.root.join(CURRENT_ENTRY);
        if let Some(active) = self.active_release()?
            && active.release_id == release.release_id
            && active.commit == release.commit
        {
            sync_directory(&self.root)?;
            self.validate_live_storage()?;
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
        let active = self
            .active_release()?
            .ok_or(ReleaseError::InvalidCurrentEntry)?;
        if active.release_id != release.release_id || active.commit != release.commit {
            return Err(ReleaseError::ActivationConflict);
        }
        Ok(active)
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
        let active_id = components
            .next()
            .and_then(|value| value.as_os_str().to_str())
            .filter(|_| components.next().is_none())
            .ok_or(ReleaseError::InvalidCurrentEntry)?;
        let manifest = read_manifest(&self.by_id.stable.join(active_id))?;
        if manifest.release_id != active_id
            || release_id(manifest.created_at, &manifest.commit) != active_id
        {
            return Err(ReleaseError::InvalidManifest);
        }
        validate_commit(&manifest.commit)?;
        validate_release(&self.by_id.stable.join(active_id), &manifest, self.policy)?;
        Ok(Some(ActiveRelease {
            release_id: manifest.release_id,
            commit: manifest.commit,
        }))
    }

    /// Finds the newest validated prepared release for a committed revision.
    pub fn prepared_for_commit(
        &self,
        commit: &str,
    ) -> Result<Option<PreparedRelease>, ReleaseError> {
        self.validate_live_storage()?;
        validate_commit(commit)?;
        let reference = self.by_commit.stable.join(commit);
        let metadata = match fs::symlink_metadata(&reference) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(ReleaseError::Io(error)),
        };
        if !metadata.file_type().is_symlink() {
            return Err(ReleaseError::InvalidCommitReference);
        }
        let target = fs::read_link(&reference).map_err(ReleaseError::Io)?;
        let release_id = release_id_from_commit_target(&target)?;
        let release_path = self.by_id.stable.join(release_id);
        let manifest = read_manifest(&release_path)?;
        if manifest.commit != commit || manifest.release_id != release_id {
            return Err(ReleaseError::InvalidCommitReference);
        }
        validate_release(&release_path, &manifest, self.policy)?;
        Ok(Some(PreparedRelease {
            release_id: manifest.release_id,
            commit: manifest.commit,
        }))
    }

    /// Rebuilds the derived commit reference from a validated release ID.
    pub fn repair_commit_reference(
        &self,
        release_id: &str,
    ) -> Result<PreparedRelease, ReleaseError> {
        let _mutation = self.lock_mutation()?;
        self.validate_live_storage()?;
        validate_release_id_component(release_id)?;
        let release_path = self.by_id.stable.join(release_id);
        let manifest = read_manifest(&release_path)?;
        if manifest.release_id != release_id {
            return Err(ReleaseError::InvalidManifest);
        }
        validate_release(&release_path, &manifest, self.policy)?;
        self.ensure_commit_reference(&manifest)?;
        Ok(PreparedRelease {
            release_id: manifest.release_id,
            commit: manifest.commit,
        })
    }

    fn cleanup_recovered_staging(
        &self,
        batch_id: BatchId,
        batch: &BatchLease,
        expected: &ReleaseManifest,
    ) -> Result<(), ReleaseError> {
        let mut entries = fs::read_dir(&batch.stable).map_err(ReleaseError::Io)?;
        match entries.next().transpose().map_err(ReleaseError::Io)? {
            Some(entry) if entry.file_name().as_os_str() == SITE_DIRECTORY => {
                if entries
                    .next()
                    .transpose()
                    .map_err(ReleaseError::Io)?
                    .is_some()
                {
                    return Err(ReleaseError::RecoveredBuildConflict);
                }
                validate_release(&entry.path(), expected, self.policy)?;
            }
            Some(_) => return Err(ReleaseError::RecoveredBuildConflict),
            None => {}
        }
        self.remove_batch_directory(batch_id, batch)
    }

    fn remove_batch_directory(
        &self,
        batch_id: BatchId,
        batch: &BatchLease,
    ) -> Result<(), ReleaseError> {
        validate_pinned_directory(&batch.configured, &batch.handle)?;
        let private = self
            .staging
            .stable
            .join(format!(".cleanup-{batch_id}-{}", Ulid::generate()));
        fs::rename(&batch.configured, &private).map_err(ReleaseError::Io)?;
        sync_directory(&self.staging.stable)?;
        validate_pinned_directory(&private, &batch.handle)?;
        fs::remove_dir_all(&private).map_err(ReleaseError::Io)?;
        sync_directory(&self.staging.stable)?;
        self.validate_live_storage()
    }

    fn batch_intent(&self, batch_id: BatchId) -> Result<Option<String>, ReleaseError> {
        let reference = self.by_batch.stable.join(batch_id.to_string());
        let metadata = match fs::symlink_metadata(&reference) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(ReleaseError::Io(error)),
        };
        if !metadata.file_type().is_symlink() {
            return Err(ReleaseError::InvalidBatchIntent);
        }
        let target = fs::read_link(reference).map_err(ReleaseError::Io)?;
        let release_id =
            release_id_from_commit_target(&target).map_err(|_| ReleaseError::InvalidBatchIntent)?;
        Ok(Some(release_id.into()))
    }

    fn ensure_batch_intent(
        &self,
        batch_id: BatchId,
        manifest: &ReleaseManifest,
    ) -> Result<(), ReleaseError> {
        let target = PathBuf::from("..")
            .join(BY_ID_DIRECTORY)
            .join(&manifest.release_id);
        let reference = self.by_batch.stable.join(batch_id.to_string());
        if let Ok(metadata) = fs::symlink_metadata(&reference)
            && metadata.file_type().is_symlink()
            && fs::read_link(&reference).map_err(ReleaseError::Io)? == target
        {
            sync_directory(&self.by_batch.stable)?;
            return self.validate_live_storage();
        }
        replace_symlink(&self.by_batch.stable, &reference, &target, "batch")?;
        self.validate_live_storage()
    }

    fn remove_batch_intent(&self, batch_id: BatchId) -> Result<(), ReleaseError> {
        let reference = self.by_batch.stable.join(batch_id.to_string());
        match fs::symlink_metadata(&reference) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                fs::remove_file(reference).map_err(ReleaseError::Io)?;
            }
            Ok(_) => return Err(ReleaseError::InvalidBatchIntent),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(ReleaseError::Io(error)),
        }
        sync_directory(&self.by_batch.stable)?;
        self.validate_live_storage()
    }

    fn ensure_commit_reference(&self, manifest: &ReleaseManifest) -> Result<(), ReleaseError> {
        let target = PathBuf::from("..")
            .join(BY_ID_DIRECTORY)
            .join(&manifest.release_id);
        let reference = self.by_commit.stable.join(&manifest.commit);
        match fs::symlink_metadata(&reference) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    let existing_target = fs::read_link(&reference).map_err(ReleaseError::Io)?;
                    if existing_target == target {
                        sync_directory(&self.by_commit.stable)?;
                        self.validate_live_storage()?;
                        return Ok(());
                    }
                    if let Ok(existing_id) = release_id_from_commit_target(&existing_target) {
                        let existing_path = self.by_id.stable.join(existing_id);
                        match read_manifest(&existing_path) {
                            Ok(existing)
                                if existing.commit == manifest.commit
                                    && existing.release_id == existing_id =>
                            {
                                match validate_release(&existing_path, &existing, self.policy) {
                                    Ok(()) if existing.release_id > manifest.release_id => {
                                        sync_directory(&self.by_commit.stable)?;
                                        self.validate_live_storage()?;
                                        return Ok(());
                                    }
                                    Ok(()) => {}
                                    Err(error) if derived_reference_is_repairable(&error) => {}
                                    Err(error) => return Err(error),
                                }
                            }
                            Ok(_) => {}
                            Err(error) if derived_reference_is_repairable(&error) => {}
                            Err(error) => return Err(error),
                        }
                    }
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(ReleaseError::Io(error)),
        }
        replace_symlink(&self.by_commit.stable, &reference, &target, "commit")?;
        self.validate_live_storage()
    }

    fn ensure_binding(&self) -> Result<(), ReleaseError> {
        let expected = binding_bytes(
            &self.configured_root,
            &self.root_handle,
            [&self.by_id, &self.by_commit, &self.by_batch, &self.staging],
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
        validate_pinned_directory(&self.by_commit.configured, &self.by_commit.handle)?;
        validate_pinned_directory(&self.by_batch.configured, &self.by_batch.handle)?;
        validate_pinned_directory(&self.staging.configured, &self.staging.handle)?;
        let expected = binding_bytes(
            &self.configured_root,
            &self.root_handle,
            [&self.by_id, &self.by_commit, &self.by_batch, &self.staging],
        )?;
        validate_binding(&self.root.join(BINDING_FILE), &expected)
    }

    fn lock_mutation(&self) -> Result<MutationLease, ReleaseError> {
        if self
            .mutation_available
            .compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return Err(ReleaseError::ReleaseStoreBusy);
        }
        let root = match File::open(&self.root) {
            Ok(root) => root,
            Err(error) => {
                self.mutation_available.store(true, Ordering::Release);
                return Err(ReleaseError::Io(error));
            }
        };
        match root.try_lock() {
            Ok(()) => Ok(MutationLease {
                local: Arc::clone(&self.mutation_available),
                root,
            }),
            Err(TryLockError::WouldBlock) => {
                self.mutation_available.store(true, Ordering::Release);
                Err(ReleaseError::ReleaseStoreBusy)
            }
            Err(TryLockError::Error(error)) => {
                self.mutation_available.store(true, Ordering::Release);
                Err(ReleaseError::Io(error))
            }
        }
    }
}

fn derived_reference_is_repairable(error: &ReleaseError) -> bool {
    match error {
        ReleaseError::InvalidDirectory(_)
        | ReleaseError::InvalidManifest
        | ReleaseError::ManifestDecoding(_)
        | ReleaseError::EmptyOutput
        | ReleaseError::UnsafeOutput(_) => true,
        ReleaseError::Io(error) => error.kind() == io::ErrorKind::NotFound,
        _ => false,
    }
}

#[derive(Debug)]
struct MutationLease {
    local: Arc<AtomicBool>,
    root: File,
}

impl MutationLease {
    fn validate_root(&self, expected: &File) -> Result<(), ReleaseError> {
        if same_metadata(
            &self.root.metadata().map_err(ReleaseError::Io)?,
            &expected.metadata().map_err(ReleaseError::Io)?,
        ) {
            Ok(())
        } else {
            Err(ReleaseError::StorageBindingMismatch)
        }
    }
}

impl Drop for MutationLease {
    fn drop(&mut self) {
        let _ = self.root.unlock();
        self.local.store(true, Ordering::Release);
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ReleaseManifest {
    schema_version: u16,
    release_id: String,
    commit: String,
    content_revision: Revision,
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

fn release_id_from_commit_target(target: &Path) -> Result<&str, ReleaseError> {
    use std::path::Component;

    let mut components = target.components();
    if components.next() != Some(Component::ParentDir)
        || components.next() != Some(Component::Normal(BY_ID_DIRECTORY.as_ref()))
    {
        return Err(ReleaseError::InvalidCommitReference);
    }
    let release_id = match components.next() {
        Some(Component::Normal(value)) => {
            value.to_str().ok_or(ReleaseError::InvalidCommitReference)?
        }
        _ => return Err(ReleaseError::InvalidCommitReference),
    };
    if components.next().is_some() {
        return Err(ReleaseError::InvalidCommitReference);
    }
    Ok(release_id)
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

fn validate_release_id_component(release_id: &str) -> Result<(), ReleaseError> {
    use std::path::Component;

    let mut components = Path::new(release_id).components();
    if release_id.len() <= 128
        && matches!(components.next(), Some(Component::Normal(_)))
        && components.next().is_none()
    {
        Ok(())
    } else {
        Err(ReleaseError::InvalidManifest)
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
            if read_manifest(release)? == *manifest {
                Ok(())
            } else {
                Err(ReleaseError::InvalidManifest)
            }
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
    validate_regular_file(&path, &metadata)?;
    let file = File::open(&path).map_err(ReleaseError::Io)?;
    let opened = file.metadata().map_err(ReleaseError::Io)?;
    if !same_metadata(&metadata, &opened)
        || !opened.file_type().is_file()
        || opened.len() > MAXIMUM_MANIFEST_BYTES
    {
        return Err(ReleaseError::InvalidManifest);
    }
    validate_regular_file(&path, &opened)?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(MAXIMUM_MANIFEST_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(ReleaseError::Io)?;
    if bytes.len() as u64 > MAXIMUM_MANIFEST_BYTES || bytes.len() as u64 != opened.len() {
        return Err(ReleaseError::InvalidManifest);
    }
    let manifest: ReleaseManifest =
        serde_json::from_slice(&bytes).map_err(ReleaseError::ManifestDecoding)?;
    if manifest.schema_version != MANIFEST_SCHEMA_VERSION {
        return Err(ReleaseError::InvalidManifest);
    }
    Ok(manifest)
}

fn validate_release(
    release: &Path,
    expected: &ReleaseManifest,
    policy: ReleasePolicy,
) -> Result<(), ReleaseError> {
    let actual = read_manifest(release)?;
    if actual != *expected
        || release_id(actual.created_at, &actual.commit) != actual.release_id
        || validate_release_tree(release, policy, false)? != actual.content_revision
    {
        Err(ReleaseError::InvalidManifest)
    } else {
        Ok(())
    }
}

fn validate_release_tree(
    root: &Path,
    policy: ReleasePolicy,
    synchronize: bool,
) -> Result<Revision, ReleaseError> {
    ensure_real_directory(root)?;
    let mut validation = TreeValidation {
        root,
        policy,
        synchronize,
        entries: 0,
        total_bytes: 0,
        site_files: 0,
        digest: Sha256::new(),
    };
    validation
        .digest
        .update(b"agent-knowledge-release-tree-v1\0");
    validation.directory(root, true)?;
    if validation.site_files == 0 {
        return Err(ReleaseError::EmptyOutput);
    }
    Ok(Revision::from_bytes(validation.digest.finalize().into()))
}

struct TreeValidation<'a> {
    root: &'a Path,
    policy: ReleasePolicy,
    synchronize: bool,
    entries: u64,
    total_bytes: u64,
    site_files: u64,
    digest: Sha256,
}

impl TreeValidation<'_> {
    fn directory(&mut self, directory: &Path, is_root: bool) -> Result<(), ReleaseError> {
        let listed = fs::symlink_metadata(directory).map_err(ReleaseError::Io)?;
        let directory_handle = File::open(directory).map_err(ReleaseError::Io)?;
        let opened = directory_handle.metadata().map_err(ReleaseError::Io)?;
        if !listed.file_type().is_dir()
            || !opened.file_type().is_dir()
            || !same_metadata(&listed, &opened)
        {
            return Err(ReleaseError::UnsafeOutput(directory.into()));
        }
        let mut children = Vec::new();
        for entry in fs::read_dir(directory).map_err(ReleaseError::Io)? {
            let path = entry.map_err(ReleaseError::Io)?.path();
            let is_root_manifest =
                is_root && path.file_name().and_then(|name| name.to_str()) == Some(MANIFEST_FILE);
            if !is_root_manifest {
                self.entries = self
                    .entries
                    .checked_add(1)
                    .ok_or(ReleaseError::OutputTooLarge)?;
                if self.entries > self.policy.maximum_entries {
                    return Err(ReleaseError::OutputTooLarge);
                }
            }
            children.push(path);
        }
        children.sort_unstable();
        for path in children {
            let is_root_manifest =
                is_root && path.file_name().and_then(|name| name.to_str()) == Some(MANIFEST_FILE);
            if is_root_manifest {
                self.manifest(&path)?;
                continue;
            }
            let metadata = fs::symlink_metadata(&path).map_err(ReleaseError::Io)?;
            let relative = path
                .strip_prefix(self.root)
                .map_err(|_| ReleaseError::UnsafeOutput(path.clone()))?;
            if metadata.file_type().is_dir() {
                self.hash_entry(b'd', relative);
                self.directory(&path, false)?;
            } else if metadata.file_type().is_file() {
                self.hash_entry(b'f', relative);
                self.file(&path, &metadata)?;
            } else {
                return Err(ReleaseError::UnsafeOutput(path));
            }
        }
        if self.synchronize {
            directory_handle.sync_all().map_err(ReleaseError::Io)?;
        }
        Ok(())
    }

    fn manifest(&self, path: &Path) -> Result<(), ReleaseError> {
        let listed = fs::symlink_metadata(path).map_err(ReleaseError::Io)?;
        validate_regular_file(path, &listed)?;
        if listed.len() > MAXIMUM_MANIFEST_BYTES {
            return Err(ReleaseError::InvalidManifest);
        }
        let file = File::open(path).map_err(ReleaseError::Io)?;
        let opened = file.metadata().map_err(ReleaseError::Io)?;
        if !opened.file_type().is_file()
            || !same_metadata(&listed, &opened)
            || opened.len() != listed.len()
        {
            return Err(ReleaseError::InvalidManifest);
        }
        validate_regular_file(path, &opened)?;
        if self.synchronize {
            file.sync_all().map_err(ReleaseError::Io)?;
        }
        Ok(())
    }

    fn file(&mut self, path: &Path, listed: &fs::Metadata) -> Result<(), ReleaseError> {
        validate_regular_file(path, listed)?;
        let mut file = File::open(path).map_err(ReleaseError::Io)?;
        let opened = file.metadata().map_err(ReleaseError::Io)?;
        if !opened.file_type().is_file() || !same_metadata(listed, &opened) {
            return Err(ReleaseError::UnsafeOutput(path.into()));
        }
        validate_regular_file(path, &opened)?;
        if opened.len() > self.policy.maximum_file_bytes {
            return Err(ReleaseError::OutputTooLarge);
        }
        self.site_files += 1;
        self.digest.update(opened.len().to_le_bytes());
        let mut observed = 0_u64;
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read = file.read(&mut buffer).map_err(ReleaseError::Io)?;
            if read == 0 {
                break;
            }
            observed = observed
                .checked_add(read as u64)
                .ok_or(ReleaseError::OutputTooLarge)?;
            if observed > self.policy.maximum_file_bytes {
                return Err(ReleaseError::OutputTooLarge);
            }
            self.total_bytes = self
                .total_bytes
                .checked_add(read as u64)
                .ok_or(ReleaseError::OutputTooLarge)?;
            if self.total_bytes > self.policy.maximum_total_bytes {
                return Err(ReleaseError::OutputTooLarge);
            }
            self.digest.update(&buffer[..read]);
        }
        let finished = file.metadata().map_err(ReleaseError::Io)?;
        if observed != opened.len()
            || finished.len() != opened.len()
            || !same_metadata(&opened, &finished)
        {
            return Err(ReleaseError::OutputChanged);
        }
        if self.synchronize {
            file.sync_all().map_err(ReleaseError::Io)?;
        }
        Ok(())
    }

    fn hash_entry(&mut self, kind: u8, relative: &Path) {
        let path_bytes = relative.as_os_str().as_encoded_bytes();
        self.digest.update([kind]);
        self.digest.update((path_bytes.len() as u64).to_le_bytes());
        self.digest.update(path_bytes);
    }
}

fn validate_regular_file(path: &Path, metadata: &fs::Metadata) -> Result<(), ReleaseError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(ReleaseError::UnsafeOutput(path.into()));
        }
    }
    Ok(())
}

fn lock_batch_directory(configured: &Path, path: &Path) -> Result<BatchLease, ReleaseError> {
    ensure_real_directory(path)?;
    let handle = Arc::new(File::open(path).map_err(ReleaseError::Io)?);
    lock_file(&handle)?;
    let stable = stable_directory_path(&handle, configured)?;
    validate_pinned_directory(configured, &handle)?;
    Ok(BatchLease {
        configured: configured.into(),
        stable,
        handle,
    })
}

fn lock_file(file: &File) -> Result<(), ReleaseError> {
    match file.try_lock() {
        Ok(()) => Ok(()),
        Err(TryLockError::WouldBlock) => Err(ReleaseError::BuildInProgress),
        Err(TryLockError::Error(error)) => Err(ReleaseError::Io(error)),
    }
}

fn pin_directory(configured: PathBuf, stable: PathBuf) -> Result<PinnedDirectory, ReleaseError> {
    ensure_directory_target(&stable)?;
    let handle = Arc::new(File::open(&stable).map_err(ReleaseError::Io)?);
    let stable = stable_directory_path(&handle, &configured)?;
    Ok(PinnedDirectory {
        handle,
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
    let mut bytes = b"agent-knowledge-release-store-v3\0".to_vec();
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

fn replace_symlink(
    directory: &Path,
    destination: &Path,
    target: &Path,
    kind: &str,
) -> Result<(), ReleaseError> {
    let temporary = directory.join(format!(".{kind}-{}", Ulid::generate()));
    create_symlink(target, &temporary)?;
    if let Err(error) = fs::rename(&temporary, destination) {
        let _ = fs::remove_file(&temporary);
        return Err(ReleaseError::Io(error));
    }
    sync_directory(directory)
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
    InvalidCommitReference,
    InvalidCurrentEntry,
    ActivationConflict,
    InvalidMountMetadata,
    CrossMountStorage,
    StorageBindingMismatch,
    ReleaseStoreBusy,
    BuildInProgress,
    BuildRecoveryRequired,
    RecoveredBuildConflict,
    InvalidBatchIntent,
    MissingRecoveryState,
    BuildAlreadyExists(BatchId),
    OutputTooLarge,
    OutputChanged,
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
            Self::InvalidCommitReference => {
                formatter.write_str("release commit reference is invalid")
            }
            Self::InvalidCurrentEntry => formatter.write_str("current release entry is invalid"),
            Self::ActivationConflict => {
                formatter.write_str("another release won concurrent activation")
            }
            Self::InvalidMountMetadata => formatter.write_str("mount metadata is invalid"),
            Self::CrossMountStorage => formatter.write_str("release directories cross mounts"),
            Self::StorageBindingMismatch => formatter.write_str("release storage binding changed"),
            Self::ReleaseStoreBusy => formatter.write_str("release store is busy"),
            Self::BuildInProgress => formatter.write_str("release build is still in progress"),
            Self::BuildRecoveryRequired => {
                formatter.write_str("release build has a durable recovery intent")
            }
            Self::RecoveredBuildConflict => {
                formatter.write_str("recovered release build conflicts with its prepared release")
            }
            Self::InvalidBatchIntent => formatter.write_str("release batch intent is invalid"),
            Self::MissingRecoveryState => formatter.write_str("release recovery state is missing"),
            Self::BuildAlreadyExists(batch_id) => {
                write!(
                    formatter,
                    "release build already exists for batch {batch_id}"
                )
            }
            Self::OutputTooLarge => formatter.write_str("release output exceeds configured limits"),
            Self::OutputChanged => formatter.write_str("release output changed during validation"),
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

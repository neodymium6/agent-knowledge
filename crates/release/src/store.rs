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
const CLEANUP_INTENT_DIRECTORY: &str = "cleanup-intent";
const STAGING_DIRECTORY: &str = ".staging";
const SITE_DIRECTORY: &str = "site";
const CURRENT_ENTRY: &str = "current";
const BINDING_FILE: &str = ".release-store-binding-v5";
const LEGACY_BINDING_FILE: &str = ".release-store-binding-v4";
const MANIFEST_FILE: &str = ".agent-knowledge-release.json";
const MANIFEST_TEMPORARY_FILE: &str = ".agent-knowledge-release.json.next";
const CLEANUP_MARKER_FILE: &str = ".agent-knowledge-cleanup";
const MANIFEST_SCHEMA_VERSION: u16 = 2;
const MAXIMUM_MANIFEST_BYTES: u64 = 16 * 1024;
pub(crate) const MAXIMUM_RELEASE_TREE_DEPTH: usize = 64;
const MAXIMUM_CLEANUP_ACTIONS: usize = 256;
const MAXIMUM_CLEANUP_DESCRIPTOR_DEPTH: usize = 32;

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
    pub(crate) fn validate(self) -> Result<Self, ReleaseError> {
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

/// A build directory that completed Quartz execution and final validation.
#[derive(Debug)]
pub struct BuiltDirectory(BuildDirectory);

impl BuiltDirectory {
    pub(crate) fn new(build: BuildDirectory) -> Self {
        Self(build)
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        self.0.path()
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
    cleanup_intent: PinnedDirectory,
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
    cleanup_started: bool,
    private: bool,
}

impl ReleaseStore {
    /// Creates fixed release directories and binds them to this storage root.
    pub fn open(root: impl AsRef<Path>, policy: ReleasePolicy) -> Result<Self, ReleaseError> {
        let policy = policy.validate()?;
        ensure_or_create_directory(root.as_ref())?;
        let root_handle = Arc::new(open_directory(root.as_ref())?);
        let configured_root = fs::canonicalize(root).map_err(ReleaseError::Io)?;
        validate_pinned_directory(&configured_root, &root_handle)?;
        let root = stable_directory_path(&root_handle, &configured_root)?;
        ensure_or_create_directory(&root.join(BY_ID_DIRECTORY))?;
        ensure_or_create_directory(&root.join(BY_COMMIT_DIRECTORY))?;
        ensure_or_create_directory(&root.join(BY_BATCH_DIRECTORY))?;
        ensure_or_create_directory(&root.join(CLEANUP_INTENT_DIRECTORY))?;
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
        let cleanup_intent = pin_directory(
            configured_root.join(CLEANUP_INTENT_DIRECTORY),
            root.join(CLEANUP_INTENT_DIRECTORY),
        )?;
        let staging = pin_directory(
            configured_root.join(STAGING_DIRECTORY),
            root.join(STAGING_DIRECTORY),
        )?;
        validate_common_mount(
            &root_handle,
            [&by_id, &by_commit, &by_batch, &cleanup_intent, &staging],
        )?;
        let store = Self {
            configured_root,
            root,
            root_handle,
            by_id,
            by_commit,
            by_batch,
            cleanup_intent,
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
        if path_exists(&self.cleanup_path(batch_id))? || self.cleanup_intent_exists(batch_id)? {
            return Err(ReleaseError::BuildRecoveryRequired);
        }
        match fs::create_dir(&path) {
            Ok(()) => {
                let handle = Arc::new(open_directory(&path)?);
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
        match self.lock_recoverable_batch(batch_id)? {
            Some(batch) => {
                self.remove_batch_directory(batch_id, &batch)?;
                self.validate_live_storage()
            }
            None => {
                sync_directory(&self.staging.stable)?;
                self.remove_cleanup_intent(batch_id)?;
                self.validate_live_storage()
            }
        }
    }

    /// Validates and durably promotes generated output into `by-id/`.
    pub fn prepare(
        &self,
        build: BuiltDirectory,
        commit: &str,
        created_at: OffsetDateTime,
    ) -> Result<PreparedRelease, ReleaseError> {
        let BuiltDirectory(build) = build;
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
            cleanup_started: false,
            private: false,
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
        let batch = self.lock_recoverable_batch(batch_id)?;
        let intent = self.batch_intent(batch_id)?;
        if intent.is_none() {
            if let Some(prepared) = self.prepared_for_commit(commit)? {
                let manifest = read_manifest(&self.by_id.stable.join(&prepared.release_id))?;
                if let Some(batch) = batch.as_ref() {
                    self.cleanup_recovered_staging(batch_id, batch, &manifest)?;
                } else {
                    sync_directory(&self.staging.stable)?;
                    self.remove_cleanup_intent(batch_id)?;
                }
                self.remove_batch_intent(batch_id)?;
                return Ok(prepared);
            }
            if batch.is_none() {
                sync_directory(&self.staging.stable)?;
                return Err(ReleaseError::MissingRecoveryState);
            }
        }
        if let Some(intent) = intent.as_ref() {
            let destination = self.by_id.stable.join(&intent.release_id);
            if path_exists(&destination)? {
                let manifest = read_manifest(&destination)?;
                if manifest != *intent || manifest.commit != commit {
                    return Err(ReleaseError::InvalidBatchIntent);
                }
                validate_release(&destination, &self.by_id.handle, &manifest, self.policy)?;
                sync_directory(&self.by_id.stable)?;
                self.ensure_commit_reference(&manifest)?;
                if let Some(batch) = batch.as_ref() {
                    self.cleanup_recovered_staging(batch_id, batch, &manifest)?;
                } else {
                    sync_directory(&self.staging.stable)?;
                    self.remove_cleanup_intent(batch_id)?;
                }
                self.remove_batch_intent(batch_id)?;
                return Ok(PreparedRelease {
                    release_id: manifest.release_id,
                    commit: manifest.commit,
                });
            }
        }
        let batch = batch.ok_or(ReleaseError::MissingRecoveryState)?;
        if batch.cleanup_started {
            return Err(ReleaseError::InvalidBatchIntent);
        }
        let intent = intent.ok_or(ReleaseError::MissingRecoveryState)?;
        if intent.commit != commit {
            return Err(ReleaseError::InvalidBatchIntent);
        }
        self.prepare_batch(batch_id, batch, commit, intent.created_at)
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
            validate_release(&destination, &self.by_id.handle, &manifest, self.policy)?;
            if validate_release_tree_at(&destination, &self.by_id.handle, self.policy, true)?
                != manifest.content_revision
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
        let content_revision =
            validate_release_tree_at(&staging, &batch.handle, self.policy, false)?;
        let manifest = ReleaseManifest {
            schema_version: MANIFEST_SCHEMA_VERSION,
            release_id: release_id.clone(),
            commit: commit.into(),
            content_revision,
            created_at,
        };
        ensure_real_directory(&staging)?;
        match self.batch_intent(batch_id)? {
            Some(intent) if intent == manifest => {}
            Some(_) => return Err(ReleaseError::InvalidBatchIntent),
            None => {
                ensure_reserved_manifest_absent(&staging)?;
                ensure_manifest_temporary_absent(&batch.stable)?;
                self.ensure_batch_intent(batch_id, &manifest)?;
            }
        }
        publish_trusted_manifest(&batch.stable, &staging.join(MANIFEST_FILE), &manifest)?;
        if validate_release_tree_at(&staging, &batch.handle, self.policy, true)?
            != manifest.content_revision
        {
            return Err(ReleaseError::OutputChanged);
        }
        fs::rename(&staging, &destination).map_err(ReleaseError::Io)?;
        // A recovered process must observe the destination before it may
        // observe the source removal as durable.
        sync_directory(&self.by_id.stable)?;
        self.ensure_commit_reference(&manifest)?;
        self.remove_batch_directory(batch_id, &batch)?;
        self.remove_batch_intent(batch_id)?;
        self.validate_live_storage()?;
        validate_release(&destination, &self.by_id.handle, &manifest, self.policy)?;
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
        validate_release(&release_path, &self.by_id.handle, &manifest, self.policy)?;
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
        validate_release(
            &self.by_id.stable.join(active_id),
            &self.by_id.handle,
            &manifest,
            self.policy,
        )?;
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
        validate_release(&release_path, &self.by_id.handle, &manifest, self.policy)?;
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
        validate_release(&release_path, &self.by_id.handle, &manifest, self.policy)?;
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
        if batch.cleanup_started {
            return self.remove_batch_directory(batch_id, batch);
        }
        remove_stale_manifest_temporary(&batch.stable)?;
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
                validate_release(&entry.path(), &batch.handle, expected, self.policy)?;
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
        let private = self.cleanup_path(batch_id);
        self.ensure_cleanup_intent(batch_id, &batch.handle)?;
        if batch.private {
            validate_pinned_directory(&private, &batch.handle)?;
        } else {
            ensure_cleanup_marker(&batch.stable, batch_id)?;
            validate_pinned_directory(&batch.configured, &batch.handle)?;
            if path_exists(&private)? {
                return Err(ReleaseError::RecoveredBuildConflict);
            }
            fs::rename(&batch.configured, &private).map_err(ReleaseError::Io)?;
            sync_directory(&self.staging.stable)?;
            validate_pinned_directory(&private, &batch.handle)?;
        }
        validate_same_mount(&self.staging.handle, &batch.handle)?;
        clear_cleanup_directory(&batch.handle, batch_id)?;
        validate_pinned_directory(&private, &batch.handle)?;
        remove_empty_directory_at(&self.staging.handle, &cleanup_name(batch_id), &private)?;
        sync_directory(&self.staging.stable)?;
        self.remove_cleanup_intent(batch_id)?;
        self.validate_live_storage()
    }

    fn lock_recoverable_batch(
        &self,
        batch_id: BatchId,
    ) -> Result<Option<BatchLease>, ReleaseError> {
        let configured = self
            .configured_root
            .join(STAGING_DIRECTORY)
            .join(batch_id.to_string());
        let path = self.staging.stable.join(batch_id.to_string());
        let cleanup_configured = self
            .configured_root
            .join(STAGING_DIRECTORY)
            .join(cleanup_name(batch_id));
        let cleanup = self.cleanup_path(batch_id);
        let ordinary_exists = path_exists(&path)?;
        let cleanup_exists = path_exists(&cleanup)?;
        if ordinary_exists && cleanup_exists {
            return Err(ReleaseError::RecoveredBuildConflict);
        }
        if ordinary_exists {
            let mut batch = lock_batch_directory(&configured, &path, false)?;
            let intent = self.cleanup_intent_exists(batch_id)?;
            let marker = cleanup_marker_exists(&batch.stable)?;
            if intent {
                self.validate_cleanup_intent(batch_id, &batch.handle)?;
                ensure_cleanup_marker(&batch.stable, batch_id)?;
                batch.cleanup_started = true;
            } else if marker {
                return Err(ReleaseError::InvalidCleanupIntent);
            }
            return Ok(Some(batch));
        }
        if cleanup_exists {
            let mut batch = lock_batch_directory(&cleanup_configured, &cleanup, true)?;
            self.validate_cleanup_intent(batch_id, &batch.handle)?;
            batch.cleanup_started = true;
            return Ok(Some(batch));
        }
        Ok(None)
    }

    fn cleanup_path(&self, batch_id: BatchId) -> PathBuf {
        self.staging.stable.join(cleanup_name(batch_id))
    }

    fn cleanup_intent_exists(&self, batch_id: BatchId) -> Result<bool, ReleaseError> {
        path_exists(&self.cleanup_intent.stable.join(batch_id.to_string()))
    }

    fn ensure_cleanup_intent(&self, batch_id: BatchId, batch: &File) -> Result<(), ReleaseError> {
        let path = self.cleanup_intent.stable.join(batch_id.to_string());
        let expected = cleanup_identity(batch)?;
        match fs::symlink_metadata(&path) {
            Ok(_) => self.validate_cleanup_intent(batch_id, batch),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                replace_regular_file(
                    &self.cleanup_intent.stable,
                    &path,
                    &expected,
                    &format!("cleanup-intent-{batch_id}"),
                )?;
                self.validate_cleanup_intent(batch_id, batch)
            }
            Err(error) => Err(ReleaseError::Io(error)),
        }
    }

    fn validate_cleanup_intent(&self, batch_id: BatchId, batch: &File) -> Result<(), ReleaseError> {
        let path = self.cleanup_intent.stable.join(batch_id.to_string());
        let actual = match read_bounded_regular_file(&path, 256) {
            Ok(actual) => actual,
            Err(ReleaseError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                return Err(ReleaseError::InvalidCleanupIntent);
            }
            Err(error) => return Err(error),
        };
        if actual == cleanup_identity(batch)? {
            Ok(())
        } else {
            Err(ReleaseError::InvalidCleanupIntent)
        }
    }

    fn remove_cleanup_intent(&self, batch_id: BatchId) -> Result<(), ReleaseError> {
        let path = self.cleanup_intent.stable.join(batch_id.to_string());
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_file() => {
                fs::remove_file(path).map_err(ReleaseError::Io)?;
            }
            Ok(_) => return Err(ReleaseError::InvalidCleanupIntent),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(ReleaseError::Io(error)),
        }
        sync_directory(&self.cleanup_intent.stable)?;
        self.validate_live_storage()
    }

    fn batch_intent(&self, batch_id: BatchId) -> Result<Option<ReleaseManifest>, ReleaseError> {
        let reference = self.by_batch.stable.join(batch_id.to_string());
        match fs::symlink_metadata(&reference) {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(ReleaseError::Io(error)),
        }
        let manifest = read_manifest_file(&reference).map_err(|error| match error {
            ReleaseError::Io(error) => ReleaseError::Io(error),
            _ => ReleaseError::InvalidBatchIntent,
        })?;
        if release_id(manifest.created_at, &manifest.commit) != manifest.release_id
            || validate_commit(&manifest.commit).is_err()
        {
            return Err(ReleaseError::InvalidBatchIntent);
        }
        Ok(Some(manifest))
    }

    fn ensure_batch_intent(
        &self,
        batch_id: BatchId,
        manifest: &ReleaseManifest,
    ) -> Result<(), ReleaseError> {
        let reference = self.by_batch.stable.join(batch_id.to_string());
        if path_exists(&reference)? {
            if self.batch_intent(batch_id)?.as_ref() == Some(manifest) {
                sync_directory(&self.by_batch.stable)?;
                return self.validate_live_storage();
            }
            return Err(ReleaseError::InvalidBatchIntent);
        }
        let mut contents = serde_json::to_vec(manifest).map_err(ReleaseError::ManifestEncoding)?;
        contents.push(b'\n');
        replace_regular_file(&self.by_batch.stable, &reference, &contents, "batch")?;
        self.validate_live_storage()
    }

    fn remove_batch_intent(&self, batch_id: BatchId) -> Result<(), ReleaseError> {
        let reference = self.by_batch.stable.join(batch_id.to_string());
        match fs::symlink_metadata(&reference) {
            Ok(metadata) if metadata.file_type().is_file() => {
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
                                match validate_release(
                                    &existing_path,
                                    &self.by_id.handle,
                                    &existing,
                                    self.policy,
                                ) {
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
        let directories = [
            &self.by_id,
            &self.by_commit,
            &self.by_batch,
            &self.cleanup_intent,
            &self.staging,
        ];
        let expected = binding_bytes(&self.configured_root, &self.root_handle, directories)?;
        let path = self.root.join(BINDING_FILE);
        match fs::symlink_metadata(&path) {
            Ok(_) => validate_binding(&path, &expected),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let legacy = self.root.join(LEGACY_BINDING_FILE);
                match fs::symlink_metadata(&legacy) {
                    Ok(_) => validate_legacy_binding(
                        &legacy,
                        &self.configured_root,
                        &self.root_handle,
                        directories,
                    )?,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        if !self.is_fresh_unbound_store()? {
                            return Err(ReleaseError::StorageBindingMismatch);
                        }
                    }
                    Err(error) => return Err(ReleaseError::Io(error)),
                }
                replace_regular_file(&self.root, &path, &expected, "store-binding")?;
                validate_binding(&path, &expected)
            }
            Err(error) => Err(ReleaseError::Io(error)),
        }
    }

    fn is_fresh_unbound_store(&self) -> Result<bool, ReleaseError> {
        for directory in [
            &self.by_id,
            &self.by_commit,
            &self.by_batch,
            &self.cleanup_intent,
            &self.staging,
        ] {
            if fs::read_dir(&directory.stable)
                .map_err(ReleaseError::Io)?
                .next()
                .transpose()
                .map_err(ReleaseError::Io)?
                .is_some()
            {
                return Ok(false);
            }
        }
        for entry in fs::read_dir(&self.root).map_err(ReleaseError::Io)? {
            let entry = entry.map_err(ReleaseError::Io)?;
            let name = entry.file_name();
            let allowed_directory = [
                BY_ID_DIRECTORY,
                BY_COMMIT_DIRECTORY,
                BY_BATCH_DIRECTORY,
                CLEANUP_INTENT_DIRECTORY,
                STAGING_DIRECTORY,
            ]
            .into_iter()
            .any(|allowed| name.as_os_str() == std::ffi::OsStr::new(allowed));
            let interrupted_binding = name
                .to_str()
                .is_some_and(|name| name.starts_with(".store-binding-"))
                && entry.file_type().map_err(ReleaseError::Io)?.is_file();
            if !allowed_directory && !interrupted_binding {
                return Ok(false);
            }
        }
        Ok(true)
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
        validate_pinned_directory(&self.cleanup_intent.configured, &self.cleanup_intent.handle)?;
        validate_pinned_directory(&self.staging.configured, &self.staging.handle)?;
        let expected = binding_bytes(
            &self.configured_root,
            &self.root_handle,
            [
                &self.by_id,
                &self.by_commit,
                &self.by_batch,
                &self.cleanup_intent,
                &self.staging,
            ],
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
        | ReleaseError::OutputTooLarge
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

#[cfg(test)]
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

#[cfg(test)]
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

fn ensure_reserved_manifest_absent(staging: &Path) -> Result<(), ReleaseError> {
    match fs::symlink_metadata(staging.join(MANIFEST_FILE)) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Ok(_) => Err(ReleaseError::InvalidManifest),
        Err(error) => Err(ReleaseError::Io(error)),
    }
}

fn ensure_manifest_temporary_absent(batch: &Path) -> Result<(), ReleaseError> {
    match fs::symlink_metadata(batch.join(MANIFEST_TEMPORARY_FILE)) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Ok(_) => Err(ReleaseError::InvalidManifest),
        Err(error) => Err(ReleaseError::Io(error)),
    }
}

fn remove_stale_manifest_temporary(batch: &Path) -> Result<(), ReleaseError> {
    let temporary = batch.join(MANIFEST_TEMPORARY_FILE);
    match fs::symlink_metadata(&temporary) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Ok(metadata) if metadata.file_type().is_file() => {
            fs::remove_file(&temporary).map_err(ReleaseError::Io)?;
            sync_directory(batch)
        }
        Ok(_) => Err(ReleaseError::InvalidManifest),
        Err(error) => Err(ReleaseError::Io(error)),
    }
}

fn publish_trusted_manifest(
    batch: &Path,
    path: &Path,
    manifest: &ReleaseManifest,
) -> Result<(), ReleaseError> {
    let mut contents = serde_json::to_vec(manifest).map_err(ReleaseError::ManifestEncoding)?;
    contents.push(b'\n');
    remove_stale_manifest_temporary(batch)?;
    let temporary = batch.join(MANIFEST_TEMPORARY_FILE);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(ReleaseError::Io)?;
    if let Err(error) = file
        .write_all(&contents)
        .and_then(|()| file.sync_all())
        .and_then(|()| fs::rename(&temporary, path))
    {
        let _ = fs::remove_file(&temporary);
        return Err(ReleaseError::Io(error));
    }
    sync_directory(path.parent().ok_or(ReleaseError::InvalidManifest)?)?;
    sync_directory(batch)
}

fn read_manifest(release: &Path) -> Result<ReleaseManifest, ReleaseError> {
    ensure_real_directory(release)?;
    let path = release.join(MANIFEST_FILE);
    read_manifest_file(&path)
}

fn read_manifest_file(path: &Path) -> Result<ReleaseManifest, ReleaseError> {
    let metadata = fs::symlink_metadata(path).map_err(ReleaseError::Io)?;
    if !metadata.file_type().is_file() || metadata.len() > MAXIMUM_MANIFEST_BYTES {
        return Err(ReleaseError::InvalidManifest);
    }
    validate_regular_file(path, &metadata)?;
    let file = open_regular_file(path)?;
    let opened = file.metadata().map_err(ReleaseError::Io)?;
    if !same_metadata(&metadata, &opened)
        || !opened.file_type().is_file()
        || opened.len() > MAXIMUM_MANIFEST_BYTES
    {
        return Err(ReleaseError::InvalidManifest);
    }
    validate_regular_file(path, &opened)?;
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
    parent: &File,
    expected: &ReleaseManifest,
    policy: ReleasePolicy,
) -> Result<(), ReleaseError> {
    let actual = read_manifest(release)?;
    if actual != *expected
        || release_id(actual.created_at, &actual.commit) != actual.release_id
        || validate_release_tree_at(release, parent, policy, false)? != actual.content_revision
    {
        Err(ReleaseError::InvalidManifest)
    } else {
        Ok(())
    }
}

#[cfg(test)]
fn validate_release_tree(
    root: &Path,
    policy: ReleasePolicy,
    synchronize: bool,
) -> Result<Revision, ReleaseError> {
    let parent_path = root
        .parent()
        .ok_or_else(|| ReleaseError::UnsafeOutput(root.into()))?;
    let parent = open_directory(parent_path)?;
    validate_release_tree_at(root, &parent, policy, synchronize)
}

fn validate_release_tree_at(
    root: &Path,
    parent: &File,
    policy: ReleasePolicy,
    synchronize: bool,
) -> Result<Revision, ReleaseError> {
    let name = root
        .file_name()
        .ok_or_else(|| ReleaseError::UnsafeOutput(root.into()))?;
    let stable_parent = stable_directory_path(
        parent,
        root.parent()
            .ok_or_else(|| ReleaseError::UnsafeOutput(root.into()))?,
    )?;
    let anchored_root = stable_parent.join(name);
    let listed = fs::symlink_metadata(&anchored_root).map_err(ReleaseError::Io)?;
    if !listed.file_type().is_dir() {
        return Err(ReleaseError::UnsafeOutput(root.into()));
    }
    let root_handle = open_directory(&anchored_root)?;
    let opened = root_handle.metadata().map_err(ReleaseError::Io)?;
    if !opened.file_type().is_dir() || !same_metadata(&listed, &opened) {
        return Err(ReleaseError::UnsafeOutput(root.into()));
    }
    validate_same_mount(parent, &root_handle)?;
    let mut validation = TreeValidation {
        root: &anchored_root,
        root_handle,
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
    let root_directory = validation
        .root_handle
        .try_clone()
        .map_err(ReleaseError::Io)?;
    validation.directory(&root_directory, Path::new(""), true, 0)?;
    validate_pinned_directory(&anchored_root, &validation.root_handle).map_err(
        |error| match error {
            ReleaseError::StorageBindingMismatch => ReleaseError::UnsafeOutput(root.into()),
            error => error,
        },
    )?;
    validate_same_mount(parent, &validation.root_handle)?;
    if validation.site_files == 0 {
        return Err(ReleaseError::EmptyOutput);
    }
    Ok(Revision::from_bytes(validation.digest.finalize().into()))
}

struct TreeValidation<'a> {
    root: &'a Path,
    root_handle: File,
    policy: ReleasePolicy,
    synchronize: bool,
    entries: u64,
    total_bytes: u64,
    site_files: u64,
    digest: Sha256,
}

impl TreeValidation<'_> {
    fn directory(
        &mut self,
        directory: &File,
        relative_directory: &Path,
        is_root: bool,
        depth: usize,
    ) -> Result<(), ReleaseError> {
        validate_same_mount(&self.root_handle, directory)?;
        let configured = self.root.join(relative_directory);
        let stable = stable_directory_path(directory, &configured)?;
        let mut children = Vec::new();
        for entry in fs::read_dir(&stable).map_err(ReleaseError::Io)? {
            let name = entry.map_err(ReleaseError::Io)?.file_name();
            let is_root_manifest =
                is_root && name.as_os_str() == std::ffi::OsStr::new(MANIFEST_FILE);
            if !is_root_manifest {
                self.entries = self
                    .entries
                    .checked_add(1)
                    .ok_or(ReleaseError::OutputTooLarge)?;
                if self.entries > self.policy.maximum_entries {
                    return Err(ReleaseError::OutputTooLarge);
                }
            }
            children.push(name);
        }
        children.sort_unstable();
        for name in children {
            let path = stable.join(&name);
            let relative = relative_directory.join(&name);
            let is_root_manifest =
                is_root && name.as_os_str() == std::ffi::OsStr::new(MANIFEST_FILE);
            if is_root_manifest {
                self.manifest(&path)?;
                continue;
            }
            let metadata = fs::symlink_metadata(&path).map_err(ReleaseError::Io)?;
            if metadata.file_type().is_dir() {
                if depth == MAXIMUM_RELEASE_TREE_DEPTH {
                    return Err(ReleaseError::OutputTooLarge);
                }
                let child = open_directory(&path)?;
                let opened = child.metadata().map_err(ReleaseError::Io)?;
                if !opened.file_type().is_dir() || !same_metadata(&metadata, &opened) {
                    return Err(ReleaseError::UnsafeOutput(relative));
                }
                validate_same_mount(&self.root_handle, &child)?;
                self.hash_entry(b'd', &relative);
                self.directory(&child, &relative, false, depth + 1)?;
                let current = fs::symlink_metadata(&path).map_err(ReleaseError::Io)?;
                if !current.file_type().is_dir() || !same_metadata(&current, &opened) {
                    return Err(ReleaseError::UnsafeOutput(relative));
                }
                let current_handle = open_directory(&path)?;
                if !same_metadata(
                    &current_handle.metadata().map_err(ReleaseError::Io)?,
                    &opened,
                ) {
                    return Err(ReleaseError::UnsafeOutput(relative));
                }
                validate_same_mount(&self.root_handle, &current_handle)?;
            } else if metadata.file_type().is_file() {
                self.hash_entry(b'f', &relative);
                self.file(&path, &relative, &metadata)?;
            } else {
                return Err(ReleaseError::UnsafeOutput(relative));
            }
        }
        if self.synchronize {
            directory.sync_all().map_err(ReleaseError::Io)?;
        }
        Ok(())
    }

    fn manifest(&self, path: &Path) -> Result<(), ReleaseError> {
        let listed = fs::symlink_metadata(path).map_err(ReleaseError::Io)?;
        validate_regular_file(path, &listed)?;
        if listed.len() > MAXIMUM_MANIFEST_BYTES {
            return Err(ReleaseError::InvalidManifest);
        }
        let file = open_regular_file(path)?;
        let opened = file.metadata().map_err(ReleaseError::Io)?;
        if !opened.file_type().is_file()
            || !same_metadata(&listed, &opened)
            || opened.len() != listed.len()
        {
            return Err(ReleaseError::InvalidManifest);
        }
        validate_regular_file(path, &opened)?;
        validate_same_mount(&self.root_handle, &file)?;
        if self.synchronize {
            file.sync_all().map_err(ReleaseError::Io)?;
        }
        let current = fs::symlink_metadata(path).map_err(ReleaseError::Io)?;
        if !current.file_type().is_file() || !same_metadata(&current, &opened) {
            return Err(ReleaseError::InvalidManifest);
        }
        let current_file = open_regular_file(path)?;
        if !same_metadata(&current_file.metadata().map_err(ReleaseError::Io)?, &opened) {
            return Err(ReleaseError::InvalidManifest);
        }
        validate_same_mount(&self.root_handle, &current_file)?;
        Ok(())
    }

    fn file(
        &mut self,
        path: &Path,
        relative: &Path,
        listed: &fs::Metadata,
    ) -> Result<(), ReleaseError> {
        validate_regular_file(path, listed)?;
        let mut file = open_regular_file(path)?;
        let opened = file.metadata().map_err(ReleaseError::Io)?;
        if !opened.file_type().is_file() || !same_metadata(listed, &opened) {
            return Err(ReleaseError::UnsafeOutput(relative.into()));
        }
        validate_regular_file(path, &opened)?;
        validate_same_mount(&self.root_handle, &file)?;
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
        let current = fs::symlink_metadata(path).map_err(ReleaseError::Io)?;
        if !current.file_type().is_file() || !same_metadata(&current, &opened) {
            return Err(ReleaseError::OutputChanged);
        }
        let current_file = open_regular_file(path)?;
        if !same_metadata(&current_file.metadata().map_err(ReleaseError::Io)?, &opened) {
            return Err(ReleaseError::OutputChanged);
        }
        validate_same_mount(&self.root_handle, &current_file)?;
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

fn lock_batch_directory(
    configured: &Path,
    path: &Path,
    private: bool,
) -> Result<BatchLease, ReleaseError> {
    ensure_real_directory(path)?;
    let handle = Arc::new(open_directory(path)?);
    lock_file(&handle)?;
    let stable = stable_directory_path(&handle, configured)?;
    validate_pinned_directory(configured, &handle)?;
    Ok(BatchLease {
        configured: configured.into(),
        stable,
        handle,
        cleanup_started: false,
        private,
    })
}

fn cleanup_name(batch_id: BatchId) -> String {
    format!(".cleanup-{batch_id}")
}

fn cleanup_marker_bytes(batch_id: BatchId) -> Vec<u8> {
    format!("agent-knowledge-release-cleanup-v1\nbatch-id={batch_id}\n").into_bytes()
}

fn cleanup_identity(directory: &File) -> Result<Vec<u8>, ReleaseError> {
    let mut bytes = b"agent-knowledge-release-cleanup-intent-v2\0".to_vec();
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        let metadata = directory.metadata().map_err(ReleaseError::Io)?;
        bytes.extend_from_slice(&metadata.ino().to_le_bytes());
    }
    Ok(bytes)
}

fn read_bounded_regular_file(path: &Path, maximum: u64) -> Result<Vec<u8>, ReleaseError> {
    let listed = fs::symlink_metadata(path).map_err(ReleaseError::Io)?;
    if !listed.file_type().is_file() || listed.len() > maximum {
        return Err(ReleaseError::InvalidCleanupIntent);
    }
    validate_regular_file(path, &listed).map_err(|_| ReleaseError::InvalidCleanupIntent)?;
    let file = open_regular_file(path)?;
    let opened = file.metadata().map_err(ReleaseError::Io)?;
    if !opened.file_type().is_file()
        || opened.len() != listed.len()
        || !same_metadata(&listed, &opened)
    {
        return Err(ReleaseError::InvalidCleanupIntent);
    }
    validate_regular_file(path, &opened).map_err(|_| ReleaseError::InvalidCleanupIntent)?;
    let mut bytes = Vec::with_capacity(opened.len() as usize);
    file.take(maximum + 1)
        .read_to_end(&mut bytes)
        .map_err(ReleaseError::Io)?;
    if bytes.len() as u64 != opened.len() {
        return Err(ReleaseError::InvalidCleanupIntent);
    }
    Ok(bytes)
}

fn ensure_cleanup_marker(directory: &Path, batch_id: BatchId) -> Result<(), ReleaseError> {
    let marker = directory.join(CLEANUP_MARKER_FILE);
    let expected = cleanup_marker_bytes(batch_id);
    match fs::symlink_metadata(&marker) {
        Ok(metadata) => {
            if !metadata.file_type().is_file() {
                return Err(ReleaseError::RecoveredBuildConflict);
            }
            validate_regular_file(&marker, &metadata)
                .map_err(|_| ReleaseError::RecoveredBuildConflict)?;
            if metadata.len() != expected.len() as u64 {
                return replace_regular_file(directory, &marker, &expected, "cleanup-marker");
            }
            let mut file = open_regular_file(&marker)?;
            let opened = file.metadata().map_err(ReleaseError::Io)?;
            if !opened.file_type().is_file()
                || !same_metadata(&metadata, &opened)
                || opened.len() != expected.len() as u64
            {
                return Err(ReleaseError::RecoveredBuildConflict);
            }
            validate_regular_file(&marker, &opened)?;
            let mut actual = Vec::with_capacity(expected.len());
            Read::by_ref(&mut file)
                .take(expected.len() as u64 + 1)
                .read_to_end(&mut actual)
                .map_err(ReleaseError::Io)?;
            if actual != expected {
                return replace_regular_file(directory, &marker, &expected, "cleanup-marker");
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return replace_regular_file(directory, &marker, &expected, "cleanup-marker");
        }
        Err(error) => return Err(ReleaseError::Io(error)),
    }
    sync_directory(directory)
}

fn cleanup_marker_exists(directory: &Path) -> Result<bool, ReleaseError> {
    let marker = directory.join(CLEANUP_MARKER_FILE);
    match fs::symlink_metadata(&marker) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(ReleaseError::Io(error)),
    }
}

#[cfg(unix)]
fn clear_cleanup_directory(directory: &File, _batch_id: BatchId) -> Result<(), ReleaseError> {
    use nix::errno::Errno;
    use nix::unistd::{UnlinkatFlags, unlinkat};

    clear_directory_at(directory, Some(CLEANUP_MARKER_FILE.as_bytes()))?;
    match unlinkat(directory, CLEANUP_MARKER_FILE, UnlinkatFlags::NoRemoveDir) {
        Ok(()) | Err(Errno::ENOENT) => {}
        Err(error) => return Err(nix_io_error(error)),
    }
    directory.sync_all().map_err(ReleaseError::Io)?;
    let stable = stable_directory_path(directory, Path::new(""))?;
    let mut entries = fs::read_dir(&stable).map_err(ReleaseError::Io)?;
    if entries
        .next()
        .transpose()
        .map_err(ReleaseError::Io)?
        .is_some()
    {
        return Err(ReleaseError::RecoveredBuildConflict);
    }
    Ok(())
}

#[cfg(unix)]
fn clear_directory_at(directory: &File, preserved_name: Option<&[u8]>) -> Result<(), ReleaseError> {
    use nix::fcntl::{AtFlags, OFlag, openat, renameat};
    use nix::sys::stat::{Mode, fstat, fstatat};
    use nix::unistd::{UnlinkatFlags, unlinkat};
    use std::ffi::CString;

    struct Frame {
        directory: File,
        name_in_parent: Option<CString>,
    }

    let root = directory.try_clone().map_err(ReleaseError::Io)?;
    let mut frames = vec![Frame {
        directory: root.try_clone().map_err(ReleaseError::Io)?,
        name_in_parent: None,
    }];
    let mut actions = 0_usize;
    loop {
        let is_root = frames.len() == 1;
        let entry_name = next_cleanup_entry(
            &frames
                .last()
                .ok_or(ReleaseError::RecoveredBuildConflict)?
                .directory,
            if is_root { preserved_name } else { None },
        )?;
        let Some(name) = entry_name else {
            frames
                .last()
                .ok_or(ReleaseError::RecoveredBuildConflict)?
                .directory
                .sync_all()
                .map_err(ReleaseError::Io)?;
            if is_root {
                return Ok(());
            }
            if actions == MAXIMUM_CLEANUP_ACTIONS {
                for frame in &frames {
                    frame.directory.sync_all().map_err(ReleaseError::Io)?;
                }
                return Err(ReleaseError::CleanupIncomplete);
            }
            let child = frames.pop().ok_or(ReleaseError::RecoveredBuildConflict)?;
            let name = child
                .name_in_parent
                .as_ref()
                .ok_or(ReleaseError::RecoveredBuildConflict)?;
            let parent = &frames
                .last()
                .ok_or(ReleaseError::RecoveredBuildConflict)?
                .directory;
            let opened = fstat(&child.directory).map_err(nix_io_error)?;
            let current = fstatat(parent, name.as_c_str(), AtFlags::AT_SYMLINK_NOFOLLOW)
                .map_err(nix_io_error)?;
            if opened.st_dev != current.st_dev || opened.st_ino != current.st_ino {
                return Err(ReleaseError::StorageBindingMismatch);
            }
            unlinkat(parent, name.as_c_str(), UnlinkatFlags::RemoveDir).map_err(nix_io_error)?;
            actions += 1;
            continue;
        };
        if actions == MAXIMUM_CLEANUP_ACTIONS {
            for frame in &frames {
                frame.directory.sync_all().map_err(ReleaseError::Io)?;
            }
            return Err(ReleaseError::CleanupIncomplete);
        }
        let parent = &frames
            .last()
            .ok_or(ReleaseError::RecoveredBuildConflict)?
            .directory;
        let listed =
            fstatat(parent, name.as_c_str(), AtFlags::AT_SYMLINK_NOFOLLOW).map_err(nix_io_error)?;
        if unix_mode_is_directory(listed.st_mode) {
            let child = openat(
                parent,
                name.as_c_str(),
                OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW,
                Mode::empty(),
            )
            .map_err(nix_io_error)?;
            let child = File::from(child);
            let opened = fstat(&child).map_err(nix_io_error)?;
            if listed.st_dev != opened.st_dev || listed.st_ino != opened.st_ino {
                return Err(ReleaseError::StorageBindingMismatch);
            }
            validate_same_mount(&root, &child)?;
            if frames.len() < MAXIMUM_CLEANUP_DESCRIPTOR_DEPTH {
                frames.push(Frame {
                    directory: child,
                    name_in_parent: Some(name),
                });
            } else {
                let work_name = CString::new(format!(".work-{}", Ulid::generate()))
                    .map_err(|_| ReleaseError::RecoveredBuildConflict)?;
                renameat(parent, name.as_c_str(), &root, work_name.as_c_str())
                    .map_err(nix_io_error)?;
                let moved = fstatat(&root, work_name.as_c_str(), AtFlags::AT_SYMLINK_NOFOLLOW)
                    .map_err(nix_io_error)?;
                if opened.st_dev != moved.st_dev || opened.st_ino != moved.st_ino {
                    return Err(ReleaseError::StorageBindingMismatch);
                }
                parent.sync_all().map_err(ReleaseError::Io)?;
                root.sync_all().map_err(ReleaseError::Io)?;
                actions += 1;
            }
        } else {
            unlinkat(parent, name.as_c_str(), UnlinkatFlags::NoRemoveDir).map_err(nix_io_error)?;
            actions += 1;
        }
    }
}

#[cfg(unix)]
fn unix_mode_is_directory(mode: nix::libc::mode_t) -> bool {
    use nix::sys::stat::SFlag;

    mode & SFlag::S_IFMT.bits() == SFlag::S_IFDIR.bits()
}

#[cfg(unix)]
fn next_cleanup_entry(
    directory: &File,
    preserved_name: Option<&[u8]>,
) -> Result<Option<std::ffi::CString>, ReleaseError> {
    use nix::dir::Dir;
    use std::ffi::CString;

    let cloned = directory.try_clone().map_err(ReleaseError::Io)?;
    let mut entries = Dir::from_fd(cloned.into()).map_err(nix_io_error)?;
    for entry in entries.iter() {
        let entry = entry.map_err(nix_io_error)?;
        let name = entry.file_name().to_bytes();
        if name != b"." && name != b".." && preserved_name != Some(name) {
            return CString::new(name)
                .map(Some)
                .map_err(|_| ReleaseError::RecoveredBuildConflict);
        }
    }
    Ok(None)
}

#[cfg(target_os = "linux")]
fn validate_same_mount(root: &File, child: &File) -> Result<(), ReleaseError> {
    if mount_id(root)? == mount_id(child)? {
        Ok(())
    } else {
        Err(ReleaseError::CrossMountStorage)
    }
}

#[cfg(all(unix, not(target_os = "linux")))]
fn validate_same_mount(root: &File, child: &File) -> Result<(), ReleaseError> {
    use std::os::unix::fs::MetadataExt;

    if root.metadata().map_err(ReleaseError::Io)?.dev()
        == child.metadata().map_err(ReleaseError::Io)?.dev()
    {
        Ok(())
    } else {
        Err(ReleaseError::CrossMountStorage)
    }
}

#[cfg(not(unix))]
fn validate_same_mount(_root: &File, _child: &File) -> Result<(), ReleaseError> {
    Ok(())
}

#[cfg(unix)]
fn nix_io_error(error: nix::errno::Errno) -> ReleaseError {
    ReleaseError::Io(io::Error::from_raw_os_error(error as i32))
}

#[cfg(not(unix))]
fn clear_cleanup_directory(directory: &File, _batch_id: BatchId) -> Result<(), ReleaseError> {
    let stable = stable_directory_path(directory, Path::new(""))?;
    let marker = stable.join(CLEANUP_MARKER_FILE);
    for entry in fs::read_dir(&stable).map_err(ReleaseError::Io)? {
        let entry = entry.map_err(ReleaseError::Io)?;
        if entry.path() != marker {
            let metadata = entry.metadata().map_err(ReleaseError::Io)?;
            if metadata.is_dir() {
                fs::remove_dir_all(entry.path()).map_err(ReleaseError::Io)?;
            } else {
                fs::remove_file(entry.path()).map_err(ReleaseError::Io)?;
            }
        }
    }
    match fs::remove_file(&marker) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(ReleaseError::Io(error)),
    }
    sync_directory(&stable)?;
    if fs::read_dir(&stable)
        .map_err(ReleaseError::Io)?
        .next()
        .transpose()
        .map_err(ReleaseError::Io)?
        .is_none()
    {
        Ok(())
    } else {
        Err(ReleaseError::RecoveredBuildConflict)
    }
}

#[cfg(unix)]
fn remove_empty_directory_at(parent: &File, name: &str, _path: &Path) -> Result<(), ReleaseError> {
    use nix::unistd::{UnlinkatFlags, unlinkat};

    unlinkat(parent, name, UnlinkatFlags::RemoveDir).map_err(nix_io_error)
}

#[cfg(not(unix))]
fn remove_empty_directory_at(_parent: &File, _name: &str, path: &Path) -> Result<(), ReleaseError> {
    fs::remove_dir(path).map_err(ReleaseError::Io)
}

fn lock_file(file: &File) -> Result<(), ReleaseError> {
    match file.try_lock() {
        Ok(()) => Ok(()),
        Err(TryLockError::WouldBlock) => Err(ReleaseError::BuildInProgress),
        Err(TryLockError::Error(error)) => Err(ReleaseError::Io(error)),
    }
}

fn pin_directory(configured: PathBuf, stable: PathBuf) -> Result<PinnedDirectory, ReleaseError> {
    let handle = Arc::new(open_directory(&stable)?);
    let stable = stable_directory_path(&handle, &configured)?;
    validate_pinned_directory(&configured, &handle)?;
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

#[cfg(unix)]
fn open_directory(path: &Path) -> Result<File, ReleaseError> {
    use std::os::unix::fs::OpenOptionsExt;

    OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW)
        .open(path)
        .map_err(ReleaseError::Io)
}

#[cfg(not(unix))]
fn open_directory(path: &Path) -> Result<File, ReleaseError> {
    File::open(path).map_err(ReleaseError::Io)
}

#[cfg(unix)]
fn open_regular_file(path: &Path) -> Result<File, ReleaseError> {
    use std::os::unix::fs::OpenOptionsExt;

    OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK)
        .open(path)
        .map_err(ReleaseError::Io)
}

#[cfg(not(unix))]
fn open_regular_file(path: &Path) -> Result<File, ReleaseError> {
    File::open(path).map_err(ReleaseError::Io)
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
        let live = open_directory(path)?;
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
    let mut bytes = b"agent-knowledge-release-store-v5\0".to_vec();
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
            bytes.extend_from_slice(&metadata.ino().to_le_bytes());
        }
    }
    Ok(bytes)
}

fn validate_binding(path: &Path, expected: &[u8]) -> Result<(), ReleaseError> {
    if read_binding_file(path)? == expected {
        Ok(())
    } else {
        Err(ReleaseError::StorageBindingMismatch)
    }
}

fn validate_legacy_binding<'a>(
    path: &Path,
    root: &Path,
    root_handle: &File,
    directories: impl IntoIterator<Item = &'a PinnedDirectory>,
) -> Result<(), ReleaseError> {
    let actual = read_binding_file(path)?;
    let mut prefix = b"agent-knowledge-release-store-v4\0".to_vec();
    prefix.extend_from_slice(root.as_os_str().as_encoded_bytes());
    if !actual.starts_with(&prefix) {
        return Err(ReleaseError::StorageBindingMismatch);
    }
    let remainder = &actual[prefix.len()..];
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        let handles: Vec<&File> = std::iter::once(root_handle)
            .chain(
                directories
                    .into_iter()
                    .map(|directory| directory.handle.as_ref()),
            )
            .collect();
        if remainder.len() != handles.len() * 17 {
            return Err(ReleaseError::StorageBindingMismatch);
        }
        for (encoded, handle) in remainder.chunks_exact(17).zip(handles) {
            let inode = handle.metadata().map_err(ReleaseError::Io)?.ino();
            if encoded[0] != 0 || encoded[9..] != inode.to_le_bytes() {
                return Err(ReleaseError::StorageBindingMismatch);
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = root_handle;
        let _ = directories;
        if !remainder.is_empty() {
            return Err(ReleaseError::StorageBindingMismatch);
        }
    }
    Ok(())
}

fn read_binding_file(path: &Path) -> Result<Vec<u8>, ReleaseError> {
    const MAXIMUM_BINDING_BYTES: u64 = 64 * 1024;
    let listed = fs::symlink_metadata(path).map_err(ReleaseError::Io)?;
    if !listed.file_type().is_file() || listed.len() > MAXIMUM_BINDING_BYTES {
        return Err(ReleaseError::StorageBindingMismatch);
    }
    validate_regular_file(path, &listed).map_err(|_| ReleaseError::StorageBindingMismatch)?;
    let file = open_regular_file(path)?;
    let opened = file.metadata().map_err(ReleaseError::Io)?;
    if !opened.file_type().is_file()
        || opened.len() != listed.len()
        || !same_metadata(&listed, &opened)
    {
        return Err(ReleaseError::StorageBindingMismatch);
    }
    validate_regular_file(path, &opened).map_err(|_| ReleaseError::StorageBindingMismatch)?;
    let mut actual = Vec::with_capacity(opened.len() as usize);
    file.take(MAXIMUM_BINDING_BYTES + 1)
        .read_to_end(&mut actual)
        .map_err(ReleaseError::Io)?;
    let current = fs::symlink_metadata(path).map_err(ReleaseError::Io)?;
    if current.file_type().is_file()
        && same_metadata(&current, &opened)
        && actual.len() as u64 == opened.len()
    {
        Ok(actual)
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

fn replace_regular_file(
    directory: &Path,
    destination: &Path,
    contents: &[u8],
    kind: &str,
) -> Result<(), ReleaseError> {
    let temporary = directory.join(format!(".{kind}-{}", Ulid::generate()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(ReleaseError::Io)?;
    if let Err(error) = file
        .write_all(contents)
        .and_then(|()| file.sync_all())
        .and_then(|()| fs::rename(&temporary, destination))
    {
        let _ = fs::remove_file(&temporary);
        return Err(ReleaseError::Io(error));
    }
    sync_directory(directory)
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
    CleanupIncomplete,
    RecoveredBuildConflict,
    InvalidBatchIntent,
    InvalidCleanupIntent,
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
            Self::CleanupIncomplete => {
                formatter.write_str("release cleanup requires another bounded pass")
            }
            Self::RecoveredBuildConflict => {
                formatter.write_str("recovered release build conflicts with its prepared release")
            }
            Self::InvalidBatchIntent => formatter.write_str("release batch intent is invalid"),
            Self::InvalidCleanupIntent => formatter.write_str("release cleanup intent is invalid"),
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

use std::fs::{self, File};
use std::io;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use serde::Serialize;

use super::{
    MANIFEST_FILE, ReleaseError, ReleaseManifest, ReleaseStore, open_directory, read_manifest_file,
    release_id as canonical_release_id, release_id_from_commit_target, remove_empty_directory_at,
    stable_directory_path, sync_directory, validate_commit, validate_pinned_directory,
    validate_release_id_component, validate_same_mount,
};

#[cfg(unix)]
use super::clear_directory_at;

const RETENTION_TOMBSTONE_PREFIX: &str = ".retention-";
const CURRENT_RETENTION_OUTCOME_VERSION: u16 = 1;
const DEFAULT_RETAINED_RELEASES: NonZeroUsize = match NonZeroUsize::new(10) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};
const DEFAULT_MAXIMUM_SCAN_ENTRIES: NonZeroUsize = match NonZeroUsize::new(10_000) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};
const DEFAULT_MAXIMUM_REMOVALS: NonZeroUsize = match NonZeroUsize::new(10) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

/// Bounds and minimum preservation rules for one release-retention pass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReleaseRetentionPolicy {
    retained_releases: NonZeroUsize,
    maximum_scan_entries: NonZeroUsize,
    maximum_removals: NonZeroUsize,
}

impl ReleaseRetentionPolicy {
    /// Creates a validated retention policy.
    ///
    /// # Errors
    ///
    /// Returns an error when any value is zero.
    pub fn new(
        retained_releases: usize,
        maximum_scan_entries: usize,
        maximum_removals: usize,
    ) -> Result<Self, ReleaseError> {
        Ok(Self {
            retained_releases: NonZeroUsize::new(retained_releases)
                .ok_or(ReleaseError::InvalidRetentionPolicy)?,
            maximum_scan_entries: NonZeroUsize::new(maximum_scan_entries)
                .ok_or(ReleaseError::InvalidRetentionPolicy)?,
            maximum_removals: NonZeroUsize::new(maximum_removals)
                .ok_or(ReleaseError::InvalidRetentionPolicy)?,
        })
    }

    /// Returns the number of newest releases preserved by policy.
    #[must_use]
    pub const fn retained_releases(self) -> NonZeroUsize {
        self.retained_releases
    }

    /// Returns the maximum directory entries inspected in one pass.
    #[must_use]
    pub const fn maximum_scan_entries(self) -> NonZeroUsize {
        self.maximum_scan_entries
    }

    /// Returns the maximum releases selected for cleanup in one pass.
    #[must_use]
    pub const fn maximum_removals(self) -> NonZeroUsize {
        self.maximum_removals
    }
}

impl Default for ReleaseRetentionPolicy {
    fn default() -> Self {
        Self {
            retained_releases: DEFAULT_RETAINED_RELEASES,
            maximum_scan_entries: DEFAULT_MAXIMUM_SCAN_ENTRIES,
            maximum_removals: DEFAULT_MAXIMUM_REMOVALS,
        }
    }
}

/// Versioned result of one bounded release-retention pass.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseRetentionOutcome {
    schema_version: u16,
    dry_run: bool,
    releases_scanned: usize,
    eligible_releases: usize,
    planned_release_ids: Vec<String>,
    removed_release_ids: Vec<String>,
    cleanup_pending_release_ids: Vec<String>,
}

impl ReleaseRetentionOutcome {
    /// Returns whether this pass intentionally avoided all mutations.
    #[must_use]
    pub const fn dry_run(&self) -> bool {
        self.dry_run
    }

    /// Returns the number of prepared releases inspected.
    #[must_use]
    pub const fn releases_scanned(&self) -> usize {
        self.releases_scanned
    }

    /// Returns the total number of releases eligible under the policy.
    #[must_use]
    pub const fn eligible_releases(&self) -> usize {
        self.eligible_releases
    }

    /// Returns the bounded release IDs selected for this pass.
    #[must_use]
    pub fn planned_release_ids(&self) -> &[String] {
        &self.planned_release_ids
    }

    /// Returns release IDs whose derived trees were completely removed.
    #[must_use]
    pub fn removed_release_ids(&self) -> &[String] {
        &self.removed_release_ids
    }

    /// Returns private tombstones that require another bounded pass.
    #[must_use]
    pub fn cleanup_pending_release_ids(&self) -> &[String] {
        &self.cleanup_pending_release_ids
    }
}

#[derive(Debug)]
struct PinnedRelease {
    release_id: String,
    commit: String,
    manifest: Option<ReleaseManifest>,
    configured: PathBuf,
    stable: PathBuf,
    handle: Arc<File>,
    retired: bool,
}

impl ReleaseStore {
    /// Selects and removes old derived Quartz releases without changing the
    /// active release, canonical content, Git history, or request queue.
    ///
    /// A non-dry-run pass first resumes private retention tombstones, then
    /// atomically retires additional releases up to the configured action
    /// bound. Large trees may require another invocation to finish cleanup.
    ///
    /// # Errors
    ///
    /// Returns an error for lock contention, invalid or replaced storage,
    /// corrupt release metadata, exhausted scan bounds, deadline expiry, or
    /// filesystem failures.
    pub fn retain_releases_until(
        &self,
        policy: ReleaseRetentionPolicy,
        dry_run: bool,
        deadline: Option<Instant>,
    ) -> Result<ReleaseRetentionOutcome, ReleaseError> {
        ensure_deadline(deadline)?;
        let _mutation = self.lock_mutation()?;
        self.validate_live_storage()?;
        let active_release_id = self.active_release()?.map(|release| release.release_id);
        let mut scan_budget = policy.maximum_scan_entries.get();
        let mut tombstones = self.retention_tombstones(&mut scan_budget, deadline)?;
        let mut releases = self.retention_releases(&mut scan_budget, deadline)?;
        let releases_scanned = releases.len();
        releases.sort_by(|left, right| right.release_id.cmp(&left.release_id));
        let retained = policy.retained_releases.get().min(releases.len());
        let mut eligible = releases.split_off(retained);
        if let Some(active) = active_release_id.as_deref() {
            eligible.retain(|release| release.release_id != active);
        }
        eligible.sort_by(|left, right| left.release_id.cmp(&right.release_id));
        tombstones.sort_by(|left, right| left.release_id.cmp(&right.release_id));
        let eligible_releases = eligible.len();

        let mut targets = tombstones;
        targets.extend(eligible);
        targets.truncate(policy.maximum_removals.get());
        let planned_release_ids = targets
            .iter()
            .map(|target| target.release_id.clone())
            .collect();
        if dry_run {
            return Ok(ReleaseRetentionOutcome {
                schema_version: CURRENT_RETENTION_OUTCOME_VERSION,
                dry_run,
                releases_scanned,
                eligible_releases,
                planned_release_ids,
                removed_release_ids: Vec::new(),
                cleanup_pending_release_ids: Vec::new(),
            });
        }

        let mut removed_release_ids = Vec::new();
        let mut cleanup_pending_release_ids = Vec::new();
        for mut target in targets {
            ensure_deadline(deadline)?;
            if !target.retired {
                target = self.retire_release(target, active_release_id.as_deref())?;
            }
            match self.clear_retention_tombstone(&target) {
                Ok(()) => removed_release_ids.push(target.release_id),
                Err(ReleaseError::CleanupIncomplete) => {
                    cleanup_pending_release_ids.push(target.release_id);
                    break;
                }
                Err(error) => return Err(error),
            }
        }
        self.validate_live_storage()?;
        ensure_deadline(deadline)?;
        Ok(ReleaseRetentionOutcome {
            schema_version: CURRENT_RETENTION_OUTCOME_VERSION,
            dry_run,
            releases_scanned,
            eligible_releases,
            planned_release_ids,
            removed_release_ids,
            cleanup_pending_release_ids,
        })
    }

    fn retention_releases(
        &self,
        scan_budget: &mut usize,
        deadline: Option<Instant>,
    ) -> Result<Vec<PinnedRelease>, ReleaseError> {
        let mut releases = Vec::new();
        for entry in fs::read_dir(&self.by_id.stable).map_err(ReleaseError::Io)? {
            consume_scan_budget(scan_budget)?;
            ensure_deadline(deadline)?;
            let entry = entry.map_err(ReleaseError::Io)?;
            let release_id = entry
                .file_name()
                .into_string()
                .map_err(|_| ReleaseError::InvalidManifest)?;
            validate_release_id_component(&release_id)?;
            let pinned = pin_release(entry.path(), release_id, false)?;
            let manifest = read_manifest_file(&pinned.stable.join(MANIFEST_FILE))?;
            if manifest.release_id != pinned.release_id
                || canonical_release_id(manifest.created_at, &manifest.commit) != pinned.release_id
            {
                return Err(ReleaseError::InvalidManifest);
            }
            validate_commit(&manifest.commit)?;
            releases.push(PinnedRelease {
                commit: manifest.commit.clone(),
                manifest: Some(manifest),
                ..pinned
            });
        }
        Ok(releases)
    }

    fn retention_tombstones(
        &self,
        scan_budget: &mut usize,
        deadline: Option<Instant>,
    ) -> Result<Vec<PinnedRelease>, ReleaseError> {
        let mut tombstones = Vec::new();
        for entry in fs::read_dir(&self.staging.stable).map_err(ReleaseError::Io)? {
            consume_scan_budget(scan_budget)?;
            ensure_deadline(deadline)?;
            let entry = entry.map_err(ReleaseError::Io)?;
            let name = match entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => continue,
            };
            let Some(release_id) = name.strip_prefix(RETENTION_TOMBSTONE_PREFIX) else {
                continue;
            };
            validate_release_id_component(release_id)?;
            let commit = commit_from_release_id(release_id)?.to_owned();
            let pinned = pin_release(entry.path(), release_id.to_owned(), true)?;
            tombstones.push(PinnedRelease {
                commit,
                manifest: None,
                ..pinned
            });
        }
        Ok(tombstones)
    }

    fn retire_release(
        &self,
        release: PinnedRelease,
        active_release_id: Option<&str>,
    ) -> Result<PinnedRelease, ReleaseError> {
        if active_release_id == Some(release.release_id.as_str()) {
            return Err(ReleaseError::ActiveReleaseRetentionConflict);
        }
        let current = pin_release(
            self.by_id.stable.join(&release.release_id),
            release.release_id.clone(),
            false,
        )?;
        if !same_file(&release.handle, &current.handle)? {
            return Err(ReleaseError::StorageBindingMismatch);
        }
        let tombstone_name = retention_tombstone_name(&release.release_id);
        let tombstone_path = self.staging.stable.join(&tombstone_name);
        match fs::symlink_metadata(&tombstone_path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Ok(_) => return Err(ReleaseError::RetentionTombstoneConflict),
            Err(error) => return Err(ReleaseError::Io(error)),
        }
        let manifest = release
            .manifest
            .as_ref()
            .ok_or(ReleaseError::InvalidManifest)?;
        self.remove_retired_commit_reference(&release.commit, &release.release_id)?;
        if let Err(error) = fs::rename(&release.configured, &tombstone_path) {
            self.ensure_commit_reference(manifest)?;
            return Err(ReleaseError::Io(error));
        }
        sync_directory(&self.by_id.stable)?;
        sync_directory(&self.staging.stable)?;
        validate_pinned_directory(&tombstone_path, &release.handle)?;
        Ok(PinnedRelease {
            configured: tombstone_path.clone(),
            stable: stable_directory_path(&release.handle, &tombstone_path)?,
            retired: true,
            ..release
        })
    }

    fn clear_retention_tombstone(&self, release: &PinnedRelease) -> Result<(), ReleaseError> {
        validate_pinned_directory(
            &self
                .staging
                .stable
                .join(retention_tombstone_name(&release.release_id)),
            &release.handle,
        )?;
        validate_same_mount(&self.staging.handle, &release.handle)?;
        self.remove_retired_commit_reference(&release.commit, &release.release_id)?;
        clear_retired_directory(&release.handle)?;
        remove_retired_manifest(&release.stable)?;
        let tombstone_name = retention_tombstone_name(&release.release_id);
        let tombstone = self.staging.stable.join(&tombstone_name);
        validate_pinned_directory(&tombstone, &release.handle)?;
        remove_empty_directory_at(&self.staging.handle, &tombstone_name, &tombstone)?;
        sync_directory(&self.staging.stable)
    }

    fn remove_retired_commit_reference(
        &self,
        commit: &str,
        release_id: &str,
    ) -> Result<(), ReleaseError> {
        let reference = self.by_commit.stable.join(commit);
        let metadata = match fs::symlink_metadata(&reference) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(ReleaseError::Io(error)),
        };
        if !metadata.file_type().is_symlink() {
            return Err(ReleaseError::InvalidCommitReference);
        }
        let target = fs::read_link(&reference).map_err(ReleaseError::Io)?;
        if release_id_from_commit_target(&target)? == release_id {
            fs::remove_file(&reference).map_err(ReleaseError::Io)?;
            sync_directory(&self.by_commit.stable)?;
        }
        Ok(())
    }
}

fn pin_release(
    path: PathBuf,
    release_id: String,
    retired: bool,
) -> Result<PinnedRelease, ReleaseError> {
    let handle = Arc::new(open_directory(&path)?);
    validate_pinned_directory(&path, &handle)?;
    let stable = stable_directory_path(&handle, &path)?;
    Ok(PinnedRelease {
        release_id,
        commit: String::new(),
        manifest: None,
        configured: path,
        stable,
        handle,
        retired,
    })
}

fn retention_tombstone_name(release_id: &str) -> String {
    format!("{RETENTION_TOMBSTONE_PREFIX}{release_id}")
}

fn commit_from_release_id(release_id: &str) -> Result<&str, ReleaseError> {
    let (_, commit) = release_id
        .rsplit_once('-')
        .ok_or(ReleaseError::InvalidManifest)?;
    validate_commit(commit)?;
    Ok(commit)
}

fn consume_scan_budget(remaining: &mut usize) -> Result<(), ReleaseError> {
    if *remaining == 0 {
        Err(ReleaseError::RetentionScanLimitExceeded)
    } else {
        *remaining -= 1;
        Ok(())
    }
}

fn ensure_deadline(deadline: Option<Instant>) -> Result<(), ReleaseError> {
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        Err(ReleaseError::OperationDeadlineExceeded)
    } else {
        Ok(())
    }
}

fn same_file(left: &File, right: &File) -> Result<bool, ReleaseError> {
    Ok(super::same_metadata(
        &left.metadata().map_err(ReleaseError::Io)?,
        &right.metadata().map_err(ReleaseError::Io)?,
    ))
}

#[cfg(unix)]
fn clear_retired_directory(directory: &File) -> Result<(), ReleaseError> {
    clear_directory_at(directory, Some(MANIFEST_FILE.as_bytes()))
}

#[cfg(not(unix))]
fn clear_retired_directory(directory: &File) -> Result<(), ReleaseError> {
    let stable = stable_directory_path(directory, Path::new(""))?;
    let mut actions = 0_usize;
    for entry in fs::read_dir(&stable).map_err(ReleaseError::Io)? {
        let entry = entry.map_err(ReleaseError::Io)?;
        if entry.file_name().as_os_str() == MANIFEST_FILE {
            continue;
        }
        if actions == super::MAXIMUM_CLEANUP_ACTIONS {
            return Err(ReleaseError::CleanupIncomplete);
        }
        let metadata = entry.file_type().map_err(ReleaseError::Io)?;
        if metadata.is_dir() {
            fs::remove_dir_all(entry.path()).map_err(ReleaseError::Io)?;
        } else {
            fs::remove_file(entry.path()).map_err(ReleaseError::Io)?;
        }
        actions += 1;
    }
    directory.sync_all().map_err(ReleaseError::Io)
}

fn remove_retired_manifest(release: &Path) -> Result<(), ReleaseError> {
    let manifest = release.join(MANIFEST_FILE);
    match fs::symlink_metadata(&manifest) {
        Ok(metadata) if metadata.file_type().is_file() => {
            fs::remove_file(&manifest).map_err(ReleaseError::Io)?;
            sync_directory(release)
        }
        Ok(_) => Err(ReleaseError::InvalidManifest),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(ReleaseError::Io(error)),
    }
}

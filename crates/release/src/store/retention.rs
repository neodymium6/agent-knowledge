use std::fs::{self, File};
use std::io;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use serde::Serialize;
use time::OffsetDateTime;

use super::{
    BY_ID_DIRECTORY, CURRENT_ENTRY, MANIFEST_FILE, ReleaseError, ReleaseManifest, ReleaseStore,
    cleanup_identity, open_directory, read_bounded_regular_file, read_manifest_file,
    release_id as canonical_release_id, release_id_from_commit_target, remove_empty_directory_at,
    replace_regular_file, replace_symlink, stable_directory_path, sync_directory, validate_commit,
    validate_pinned_directory, validate_release_id_component, validate_same_mount,
};

#[cfg(unix)]
use super::clear_directory_at;

const RETENTION_TOMBSTONE_PREFIX: &str = ".retention-";
const RETENTION_INTENT_PREFIX: &str = "retention-";
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
    created_at: Option<OffsetDateTime>,
    manifest: Option<ReleaseManifest>,
    configured: PathBuf,
    stable: PathBuf,
    handle: Arc<File>,
    retired: bool,
}

#[derive(Clone, Debug)]
struct KnownRelease {
    manifest: ReleaseManifest,
    handle: Arc<File>,
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
        ensure_retention_supported()?;
        ensure_deadline(deadline)?;
        let _mutation = self.lock_mutation()?;
        self.validate_live_storage()?;
        let active_release_id = self.retention_active_release_id(deadline)?;
        let mut scan_budget = policy.maximum_scan_entries.get();
        let mut tombstones =
            self.retention_tombstones(&mut scan_budget, deadline, active_release_id.as_deref())?;
        let mut releases = self.retention_releases(&mut scan_budget, deadline)?;
        let releases_scanned = releases.len();
        let known_releases = releases
            .iter()
            .filter_map(|release| {
                release.manifest.clone().map(|manifest| KnownRelease {
                    manifest,
                    handle: Arc::clone(&release.handle),
                })
            })
            .collect::<Vec<_>>();
        releases.sort_by(|left, right| {
            right
                .created_at
                .cmp(&left.created_at)
                .then_with(|| right.release_id.cmp(&left.release_id))
        });
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
            let cleanup_pending_release_ids = targets
                .iter()
                .filter(|target| target.retired)
                .map(|target| target.release_id.clone())
                .collect();
            return Ok(ReleaseRetentionOutcome {
                schema_version: CURRENT_RETENTION_OUTCOME_VERSION,
                dry_run,
                releases_scanned,
                eligible_releases,
                planned_release_ids,
                removed_release_ids: Vec::new(),
                cleanup_pending_release_ids,
            });
        }

        let mut removed_release_ids = Vec::new();
        let mut cleanup_pending_release_ids = Vec::new();
        for mut target in targets {
            ensure_deadline(deadline)?;
            if !target.retired {
                target =
                    self.retire_release(target, active_release_id.as_deref(), &known_releases)?;
            }
            match self.clear_retention_tombstone(
                &target,
                active_release_id.as_deref(),
                &known_releases,
                deadline,
            ) {
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
                created_at: Some(manifest.created_at),
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
        active_release_id: Option<&str>,
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
            if active_release_id == Some(release_id) {
                return Err(ReleaseError::ActiveReleaseRetentionConflict);
            }
            let commit = commit_from_release_id(release_id)?.to_owned();
            let pinned = pin_release(entry.path(), release_id.to_owned(), true)?;
            let pinned = PinnedRelease {
                commit,
                created_at: None,
                manifest: None,
                ..pinned
            };
            self.validate_retention_intent(&pinned)?;
            tombstones.push(pinned);
        }
        Ok(tombstones)
    }

    fn retire_release(
        &self,
        release: PinnedRelease,
        active_release_id: Option<&str>,
        known_releases: &[KnownRelease],
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
            .ok_or(ReleaseError::InvalidManifest)?
            .clone();
        self.ensure_retention_intent(&release)?;
        fs::rename(&release.configured, &tombstone_path).map_err(ReleaseError::Io)?;
        sync_directory(&self.by_id.stable)?;
        sync_directory(&self.staging.stable)?;
        validate_pinned_directory(&tombstone_path, &release.handle)?;
        let retired = PinnedRelease {
            configured: tombstone_path.clone(),
            stable: stable_directory_path(&release.handle, &tombstone_path)?,
            retired: true,
            ..release
        };
        self.validate_retention_intent(&retired)?;
        self.repoint_retired_commit_reference(
            &manifest.commit,
            &manifest.release_id,
            known_releases,
        )?;
        Ok(retired)
    }

    fn clear_retention_tombstone(
        &self,
        release: &PinnedRelease,
        active_release_id: Option<&str>,
        known_releases: &[KnownRelease],
        deadline: Option<Instant>,
    ) -> Result<(), ReleaseError> {
        if active_release_id == Some(release.release_id.as_str()) {
            return Err(ReleaseError::ActiveReleaseRetentionConflict);
        }
        self.validate_retention_intent(release)?;
        validate_pinned_directory(
            &self
                .staging
                .stable
                .join(retention_tombstone_name(&release.release_id)),
            &release.handle,
        )?;
        validate_same_mount(&self.staging.handle, &release.handle)?;
        self.repoint_retired_commit_reference(
            &release.commit,
            &release.release_id,
            known_releases,
        )?;
        clear_retired_directory(&release.handle, deadline)?;
        ensure_deadline(deadline)?;
        remove_retired_manifest(&release.stable)?;
        let tombstone_name = retention_tombstone_name(&release.release_id);
        let tombstone = self.staging.stable.join(&tombstone_name);
        validate_pinned_directory(&tombstone, &release.handle)?;
        remove_empty_directory_at(&self.staging.handle, &tombstone_name, &tombstone)?;
        sync_directory(&self.staging.stable)?;
        self.remove_retention_intent(&release.release_id)
    }

    fn repoint_retired_commit_reference(
        &self,
        commit: &str,
        retired_release_id: &str,
        known_releases: &[KnownRelease],
    ) -> Result<(), ReleaseError> {
        let reference = self.by_commit.stable.join(commit);
        let replacement = self.newest_surviving_release(commit, known_releases)?;
        if let Some(manifest) = replacement {
            let target = PathBuf::from("..")
                .join(BY_ID_DIRECTORY)
                .join(&manifest.release_id);
            match fs::symlink_metadata(&reference) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    if fs::read_link(&reference).map_err(ReleaseError::Io)? == target {
                        return sync_directory(&self.by_commit.stable);
                    }
                }
                Ok(_) => return Err(ReleaseError::InvalidCommitReference),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(ReleaseError::Io(error)),
            }
            return replace_symlink(&self.by_commit.stable, &reference, &target, "commit");
        }

        match fs::symlink_metadata(&reference) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                let target = fs::read_link(&reference).map_err(ReleaseError::Io)?;
                if release_id_from_commit_target(&target)? != retired_release_id {
                    return Err(ReleaseError::InvalidCommitReference);
                }
                fs::remove_file(&reference).map_err(ReleaseError::Io)?;
                sync_directory(&self.by_commit.stable)?;
            }
            Ok(_) => return Err(ReleaseError::InvalidCommitReference),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(ReleaseError::Io(error)),
        }
        Ok(())
    }

    fn newest_surviving_release(
        &self,
        commit: &str,
        known_releases: &[KnownRelease],
    ) -> Result<Option<ReleaseManifest>, ReleaseError> {
        let mut candidates = known_releases
            .iter()
            .filter(|release| release.manifest.commit == commit)
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            right
                .manifest
                .created_at
                .cmp(&left.manifest.created_at)
                .then_with(|| right.manifest.release_id.cmp(&left.manifest.release_id))
        });
        for candidate in candidates {
            let path = self.by_id.stable.join(&candidate.manifest.release_id);
            match fs::symlink_metadata(&path) {
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(ReleaseError::Io(error)),
                Ok(metadata) if !metadata.file_type().is_dir() => {
                    return Err(ReleaseError::InvalidManifest);
                }
                Ok(_) => {}
            }
            validate_pinned_directory(&path, &candidate.handle)?;
            if read_manifest_file(&path.join(MANIFEST_FILE))? != candidate.manifest {
                return Err(ReleaseError::InvalidManifest);
            }
            return Ok(Some(candidate.manifest.clone()));
        }
        Ok(None)
    }

    fn retention_active_release_id(
        &self,
        deadline: Option<Instant>,
    ) -> Result<Option<String>, ReleaseError> {
        ensure_deadline(deadline)?;
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
        if components.next().and_then(|part| part.as_os_str().to_str()) != Some(BY_ID_DIRECTORY) {
            return Err(ReleaseError::InvalidCurrentEntry);
        }
        let release_id = components
            .next()
            .and_then(|part| part.as_os_str().to_str())
            .filter(|_| components.next().is_none())
            .ok_or(ReleaseError::InvalidCurrentEntry)?;
        validate_release_id_component(release_id)?;
        let manifest = read_manifest_file(&self.by_id.stable.join(release_id).join(MANIFEST_FILE))?;
        if manifest.release_id != release_id
            || canonical_release_id(manifest.created_at, &manifest.commit) != release_id
        {
            return Err(ReleaseError::InvalidManifest);
        }
        ensure_deadline(deadline)?;
        Ok(Some(release_id.to_owned()))
    }

    fn ensure_retention_intent(&self, release: &PinnedRelease) -> Result<(), ReleaseError> {
        let path = self
            .cleanup_intent
            .stable
            .join(retention_intent_name(&release.release_id));
        let expected = retention_identity(release)?;
        match fs::symlink_metadata(&path) {
            Ok(_) => self.validate_retention_intent(release),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                replace_regular_file(
                    &self.cleanup_intent.stable,
                    &path,
                    &expected,
                    "retention-intent",
                )?;
                self.validate_retention_intent(release)
            }
            Err(error) => Err(ReleaseError::Io(error)),
        }
    }

    fn validate_retention_intent(&self, release: &PinnedRelease) -> Result<(), ReleaseError> {
        let path = self
            .cleanup_intent
            .stable
            .join(retention_intent_name(&release.release_id));
        let actual = read_bounded_regular_file(&path, 512)
            .map_err(|_| ReleaseError::InvalidRetentionIntent)?;
        if actual == retention_identity(release)? {
            Ok(())
        } else {
            Err(ReleaseError::InvalidRetentionIntent)
        }
    }

    fn remove_retention_intent(&self, release_id: &str) -> Result<(), ReleaseError> {
        let path = self
            .cleanup_intent
            .stable
            .join(retention_intent_name(release_id));
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_file() => {
                fs::remove_file(path).map_err(ReleaseError::Io)?;
            }
            Ok(_) => return Err(ReleaseError::InvalidRetentionIntent),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(ReleaseError::Io(error)),
        }
        sync_directory(&self.cleanup_intent.stable)
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
        created_at: None,
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

fn retention_intent_name(release_id: &str) -> String {
    format!("{RETENTION_INTENT_PREFIX}{release_id}")
}

fn retention_identity(release: &PinnedRelease) -> Result<Vec<u8>, ReleaseError> {
    let mut identity = b"agent-knowledge-release-retention-intent-v1\0".to_vec();
    identity.extend_from_slice(release.release_id.as_bytes());
    identity.push(0);
    identity.extend_from_slice(release.commit.as_bytes());
    identity.push(0);
    identity.extend_from_slice(&cleanup_identity(&release.handle)?);
    Ok(identity)
}

#[cfg(unix)]
fn ensure_retention_supported() -> Result<(), ReleaseError> {
    Ok(())
}

#[cfg(not(unix))]
fn ensure_retention_supported() -> Result<(), ReleaseError> {
    Err(ReleaseError::RetentionUnsupported)
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
fn clear_retired_directory(
    directory: &File,
    deadline: Option<Instant>,
) -> Result<(), ReleaseError> {
    clear_directory_at(directory, Some(MANIFEST_FILE.as_bytes()), deadline)
}

#[cfg(not(unix))]
fn clear_retired_directory(
    _directory: &File,
    _deadline: Option<Instant>,
) -> Result<(), ReleaseError> {
    Err(ReleaseError::RetentionUnsupported)
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

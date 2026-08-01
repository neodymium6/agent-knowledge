use std::fmt;
use std::io;
use std::num::NonZeroUsize;
use std::path::{Component, Path, PathBuf};
use std::time::Duration as StandardDuration;
use std::time::Instant;

use agent_knowledge_core::{BoundedFileError, read_bounded_regular_file};
use agent_knowledge_repository::GitIdentity;
use serde::Deserialize;
use time::Duration;

use crate::{BatchSchedule, WorkerRunLimits};

/// Worker configuration schema supported by this release.
pub const CURRENT_WORKER_CONFIG_VERSION: u16 = 1;
const MAXIMUM_WORKER_CONFIG_BYTES: u64 = 64 * 1024;

/// Validated, side-effect-free settings for one Repository Worker process.
#[derive(Clone, Debug)]
pub struct WorkerSettings {
    queue_root: PathBuf,
    repository_root: PathBuf,
    content_root: PathBuf,
    work_root: PathBuf,
    release_root: PathBuf,
    official_branch: String,
    identity: GitIdentity,
    quartz_program: PathBuf,
    quartz_integration_root: PathBuf,
    quartz_timeout: StandardDuration,
    schedule: BatchSchedule,
    limits: WorkerRunLimits,
}

impl WorkerSettings {
    /// Loads and validates one bounded YAML configuration file.
    ///
    /// Symbolic links to regular files are supported for Kubernetes projected
    /// configuration. On Linux, the selected target is pinned without opening
    /// it for I/O, validated through that descriptor, and read only through the
    /// descriptor-backed path while the pin remains alive.
    ///
    /// # Errors
    ///
    /// Returns an error for I/O failures, non-regular files, oversized input,
    /// invalid YAML, unsupported versions, or invalid operational values.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, WorkerConfigError> {
        let bytes = read_bounded_regular_file(path, MAXIMUM_WORKER_CONFIG_BYTES)
            .map_err(WorkerConfigError::bounded_file)?;
        let yaml = std::str::from_utf8(&bytes).map_err(|_| WorkerConfigError::InvalidUtf8)?;
        Self::decode(yaml)
    }

    /// Decodes validated settings from one bounded YAML document.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed or unsupported configuration.
    pub fn decode(yaml: &str) -> Result<Self, WorkerConfigError> {
        if yaml.len() as u64 > MAXIMUM_WORKER_CONFIG_BYTES {
            return Err(WorkerConfigError::FileTooLarge {
                maximum: MAXIMUM_WORKER_CONFIG_BYTES,
            });
        }
        let budget = yaml.len().max(1);
        let options = serde_saphyr::options! {
            budget: serde_saphyr::budget! {
                max_events: budget,
                max_aliases: 0,
                max_anchors: 0,
                max_depth: 16,
                // The single-document entrypoint must observe a second
                // document in order to reject it explicitly.
                max_documents: 2,
                max_nodes: budget,
                max_total_scalar_bytes: budget,
                max_total_comment_bytes: budget,
                max_merge_keys: 0,
            },
            merge_keys: serde_saphyr::MergeKeyPolicy::Error,
            strict_booleans: true,
            with_snippet: false,
        };
        let wire: WireWorkerConfig = serde_saphyr::from_str_with_options(yaml, options)
            .map_err(|error| WorkerConfigError::InvalidYaml(Box::new(error)))?;
        Self::try_from(wire)
    }

    /// Returns the durable request queue root.
    #[must_use]
    pub fn queue_root(&self) -> &Path {
        &self.queue_root
    }

    /// Returns the bare Git repository root.
    #[must_use]
    pub fn repository_root(&self) -> &Path {
        &self.repository_root
    }

    /// Returns the canonical committed content worktree.
    #[must_use]
    pub fn content_root(&self) -> &Path {
        &self.content_root
    }

    /// Returns the disposable repository transaction root.
    #[must_use]
    pub fn work_root(&self) -> &Path {
        &self.work_root
    }

    /// Returns the immutable release store root.
    #[must_use]
    pub fn release_root(&self) -> &Path {
        &self.release_root
    }

    /// Returns the official Git branch name.
    #[must_use]
    pub fn official_branch(&self) -> &str {
        &self.official_branch
    }

    /// Returns the fixed commit author identity.
    #[must_use]
    pub const fn identity(&self) -> &GitIdentity {
        &self.identity
    }

    /// Returns the trusted Quartz executable path.
    #[must_use]
    pub fn quartz_program(&self) -> &Path {
        &self.quartz_program
    }

    /// Returns the trusted Quartz integration root.
    #[must_use]
    pub fn quartz_integration_root(&self) -> &Path {
        &self.quartz_integration_root
    }

    /// Returns the maximum duration of one Quartz invocation.
    #[must_use]
    pub const fn quartz_timeout(&self) -> StandardDuration {
        self.quartz_timeout
    }

    /// Returns the validated batch-closing thresholds.
    #[must_use]
    pub const fn schedule(&self) -> BatchSchedule {
        self.schedule
    }

    /// Returns the validated bounded-scan and batch-size limits.
    #[must_use]
    pub const fn limits(&self) -> WorkerRunLimits {
        self.limits
    }
}

impl TryFrom<WireWorkerConfig> for WorkerSettings {
    type Error = WorkerConfigError;

    fn try_from(wire: WireWorkerConfig) -> Result<Self, Self::Error> {
        if wire.schema_version != CURRENT_WORKER_CONFIG_VERSION {
            return Err(WorkerConfigError::UnsupportedSchemaVersion {
                found: wire.schema_version,
            });
        }
        let storage = [
            ("storage.queue_root", &wire.storage.queue_root),
            ("storage.repository_root", &wire.storage.repository_root),
            ("storage.content_root", &wire.storage.content_root),
            ("storage.work_root", &wire.storage.work_root),
            ("storage.release_root", &wire.storage.release_root),
        ];
        validate_storage_paths(&storage)?;
        validate_absolute_path("quartz.program", &wire.quartz.program)?;
        validate_absolute_path("quartz.integration_root", &wire.quartz.integration_root)?;
        validate_trusted_inputs(&storage, &wire.quartz)?;
        if wire.repository.official_branch.trim().is_empty()
            || wire
                .repository
                .official_branch
                .chars()
                .any(char::is_control)
        {
            return Err(WorkerConfigError::InvalidValue {
                field: "repository.official_branch",
            });
        }
        let identity =
            GitIdentity::new(&wire.repository.author_name, &wire.repository.author_email).map_err(
                |_| WorkerConfigError::InvalidValue {
                    field: "repository author identity",
                },
            )?;
        let quartz_timeout =
            positive_standard_duration("quartz.timeout_seconds", wire.quartz.timeout_seconds)?;
        let debounce =
            positive_time_duration("batch.debounce_seconds", wire.batch.debounce_seconds)?;
        let maximum_age =
            positive_time_duration("batch.maximum_age_seconds", wire.batch.maximum_age_seconds)?;
        let schedule = BatchSchedule::new(debounce, maximum_age).map_err(|_| {
            WorkerConfigError::InvalidValue {
                field: "batch schedule",
            }
        })?;
        let maximum_scan_entries = positive_usize(
            "batch.maximum_scan_entries",
            wire.batch.maximum_scan_entries,
        )?;
        let maximum_requests =
            positive_usize("batch.maximum_requests", wire.batch.maximum_requests)?;
        let maximum_recovery_requests = positive_usize(
            "batch.maximum_recovery_requests",
            wire.batch.maximum_recovery_requests,
        )?;
        if maximum_recovery_requests < maximum_requests {
            return Err(WorkerConfigError::InvalidValue {
                field: "batch.maximum_recovery_requests",
            });
        }
        Ok(Self {
            queue_root: wire.storage.queue_root,
            repository_root: wire.storage.repository_root,
            content_root: wire.storage.content_root,
            work_root: wire.storage.work_root,
            release_root: wire.storage.release_root,
            official_branch: wire.repository.official_branch,
            identity,
            quartz_program: wire.quartz.program,
            quartz_integration_root: wire.quartz.integration_root,
            quartz_timeout,
            schedule,
            limits: WorkerRunLimits::new(
                maximum_scan_entries,
                maximum_requests,
                maximum_recovery_requests,
            ),
        })
    }
}

fn validate_storage_paths(paths: &[(&'static str, &PathBuf)]) -> Result<(), WorkerConfigError> {
    for (field, path) in paths {
        validate_absolute_path(field, path)?;
    }
    for (index, (field, path)) in paths.iter().enumerate() {
        for (other_field, other_path) in &paths[index + 1..] {
            if path.starts_with(other_path) || other_path.starts_with(path) {
                return Err(WorkerConfigError::OverlappingPaths {
                    first: field,
                    second: other_field,
                });
            }
        }
    }
    Ok(())
}

fn validate_trusted_inputs(
    storage: &[(&'static str, &PathBuf)],
    quartz: &WireQuartz,
) -> Result<(), WorkerConfigError> {
    for (storage_field, storage_path) in storage {
        for (trusted_field, trusted_path) in [
            ("quartz.program", &quartz.program),
            ("quartz.integration_root", &quartz.integration_root),
        ] {
            if storage_path.starts_with(trusted_path) || trusted_path.starts_with(storage_path) {
                return Err(WorkerConfigError::OverlappingPaths {
                    first: storage_field,
                    second: trusted_field,
                });
            }
        }
    }
    Ok(())
}

fn validate_absolute_path(field: &'static str, path: &Path) -> Result<(), WorkerConfigError> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        || path.parent().is_none()
    {
        return Err(WorkerConfigError::InvalidPath { field });
    }
    Ok(())
}

fn positive_standard_duration(
    field: &'static str,
    seconds: u64,
) -> Result<StandardDuration, WorkerConfigError> {
    let duration = StandardDuration::from_secs(seconds);
    if duration.is_zero() || Instant::now().checked_add(duration).is_none() {
        return Err(WorkerConfigError::InvalidValue { field });
    }
    Ok(duration)
}

fn positive_time_duration(
    field: &'static str,
    seconds: u64,
) -> Result<Duration, WorkerConfigError> {
    let seconds = i64::try_from(seconds).map_err(|_| WorkerConfigError::InvalidValue { field })?;
    if seconds == 0 {
        return Err(WorkerConfigError::InvalidValue { field });
    }
    Ok(Duration::seconds(seconds))
}

fn positive_usize(field: &'static str, value: u64) -> Result<NonZeroUsize, WorkerConfigError> {
    let value = usize::try_from(value).map_err(|_| WorkerConfigError::InvalidValue { field })?;
    NonZeroUsize::new(value).ok_or(WorkerConfigError::InvalidValue { field })
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireWorkerConfig {
    schema_version: u16,
    storage: WireStorage,
    repository: WireRepository,
    quartz: WireQuartz,
    batch: WireBatch,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireStorage {
    queue_root: PathBuf,
    repository_root: PathBuf,
    content_root: PathBuf,
    work_root: PathBuf,
    release_root: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRepository {
    official_branch: String,
    author_name: String,
    author_email: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireQuartz {
    program: PathBuf,
    integration_root: PathBuf,
    timeout_seconds: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireBatch {
    debounce_seconds: u64,
    maximum_age_seconds: u64,
    maximum_scan_entries: u64,
    maximum_requests: u64,
    maximum_recovery_requests: u64,
}

/// Invalid Worker configuration input.
#[derive(Debug)]
pub enum WorkerConfigError {
    /// The configuration file could not be read.
    Io(io::Error),
    /// The configured path did not name a regular file.
    InvalidFileType,
    /// The configuration exceeded the fixed parser input bound.
    FileTooLarge {
        /// Maximum accepted configuration bytes.
        maximum: u64,
    },
    /// The configuration was not UTF-8.
    InvalidUtf8,
    /// YAML decoding or strict schema validation failed.
    InvalidYaml(Box<serde_saphyr::Error>),
    /// The configuration schema version is unsupported.
    UnsupportedSchemaVersion {
        /// Version found in the file.
        found: u16,
    },
    /// A path was relative, non-normalized, or an unsafe root.
    InvalidPath {
        /// Invalid configuration field.
        field: &'static str,
    },
    /// Two configured storage or trusted-input paths were equal or nested.
    OverlappingPaths {
        /// First conflicting field.
        first: &'static str,
        /// Second conflicting field.
        second: &'static str,
    },
    /// A scalar or collection limit was invalid.
    InvalidValue {
        /// Invalid configuration field.
        field: &'static str,
    },
}

impl WorkerConfigError {
    fn bounded_file(error: BoundedFileError) -> Self {
        match error {
            BoundedFileError::Io(error) => Self::Io(error),
            BoundedFileError::InvalidFileType => Self::InvalidFileType,
            BoundedFileError::FileTooLarge { maximum } => Self::FileTooLarge { maximum },
        }
    }
}

impl fmt::Display for WorkerConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "could not read Worker configuration: {error}"),
            Self::InvalidFileType => {
                formatter.write_str("Worker configuration must be a regular file")
            }
            Self::FileTooLarge { maximum } => {
                write!(formatter, "Worker configuration exceeds {maximum} bytes")
            }
            Self::InvalidUtf8 => formatter.write_str("Worker configuration must be UTF-8"),
            Self::InvalidYaml(error) => write!(formatter, "invalid Worker configuration: {error}"),
            Self::UnsupportedSchemaVersion { found } => write!(
                formatter,
                "unsupported Worker configuration schema version {found}"
            ),
            Self::InvalidPath { field } => write!(
                formatter,
                "Worker configuration path `{field}` must be absolute and normalized"
            ),
            Self::OverlappingPaths { first, second } => write!(
                formatter,
                "Worker paths `{first}` and `{second}` must not overlap"
            ),
            Self::InvalidValue { field } => {
                write!(formatter, "Worker configuration value `{field}` is invalid")
            }
        }
    }
}

impl std::error::Error for WorkerConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::InvalidYaml(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests;

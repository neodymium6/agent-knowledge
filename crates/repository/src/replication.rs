use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::{self, File, OpenOptions, TryLockError as FileTryLockError};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, TryLockError as MutexTryLockError};
use std::time::{Duration, Instant};

use agent_knowledge_core::Revision;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use ulid::Ulid;

#[cfg(unix)]
use std::os::unix::ffi::OsStringExt;

use crate::git::{
    GitRepository, GitTransactionError, open_stable_directory, run_git_for_read,
    run_git_for_read_controlled, run_git_until_controlled,
    run_git_until_controlled_with_environment, same_metadata, validate_pinned_directory,
};

const REPLICATION_STATE_VERSION: u16 = 1;
const MAXIMUM_STATE_BYTES: u64 = 64 * 1024;
const STATE_FILE_NAME: &str = "remote-replication-v1.json";
const LOCK_FILE_NAME: &str = "remote-replication.lock";
const TEMPORARY_STATE_FILE_NAME: &str = ".remote-replication.tmp";
const PUSH_URL_ENVIRONMENT: &str = "AGENT_KNOWLEDGE_PUSH_URL";
const SNAPSHOT_REMOTE: &str = "agent-knowledge-snapshot";

/// Validated retry and destination settings for asynchronous Git replication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteReplicationPolicy {
    remote: String,
    branch: String,
    timeout: Duration,
    initial_backoff: Duration,
    maximum_backoff: Duration,
}

impl RemoteReplicationPolicy {
    /// Creates a bounded remote-push policy.
    ///
    /// # Errors
    ///
    /// Returns an error when the destination or timing bounds are unsafe.
    pub fn new(
        remote: &str,
        branch: &str,
        timeout: Duration,
        initial_backoff: Duration,
        maximum_backoff: Duration,
    ) -> Result<Self, RemoteReplicationError> {
        if !valid_remote_name(remote) {
            return Err(RemoteReplicationError::InvalidPolicy("remote"));
        }
        if !valid_branch_name(branch) {
            return Err(RemoteReplicationError::InvalidPolicy("branch"));
        }
        if timeout.is_zero()
            || initial_backoff.is_zero()
            || maximum_backoff < initial_backoff
            || Instant::now().checked_add(timeout).is_none()
            || Instant::now().checked_add(maximum_backoff).is_none()
            || time::Duration::try_from(maximum_backoff).is_err()
            || OffsetDateTime::now_utc()
                .checked_add(
                    time::Duration::try_from(maximum_backoff)
                        .map_err(|_| RemoteReplicationError::InvalidPolicy("timing"))?,
                )
                .is_none()
        {
            return Err(RemoteReplicationError::InvalidPolicy("timing"));
        }
        Ok(Self {
            remote: remote.into(),
            branch: branch.into(),
            timeout,
            initial_backoff,
            maximum_backoff,
        })
    }

    /// Returns the configured Git remote name.
    #[must_use]
    pub fn remote(&self) -> &str {
        &self.remote
    }

    /// Returns the destination branch name.
    #[must_use]
    pub fn branch(&self) -> &str {
        &self.branch
    }
}

/// One bounded asynchronous replication step.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RemoteReplicationOutcome {
    /// The last confirmed remote commit already equals the official commit.
    UpToDate { commit: String },
    /// The official commit was successfully pushed and recorded.
    Pushed { commit: String },
    /// A previous failure still suppresses attempts until the durable deadline.
    Deferred {
        commit: String,
        consecutive_failures: u32,
        retry_at: OffsetDateTime,
    },
    /// This attempt failed and a later retry was durably scheduled.
    Failed {
        commit: String,
        consecutive_failures: u32,
        retry_at: OffsetDateTime,
    },
    /// Shutdown cancelled an in-flight push before it changed durable state.
    Cancelled,
}

/// Replicates the latest official local commit without participating in publication.
#[derive(Debug)]
pub struct RemoteReplicator {
    repository: GitRepository,
    policy: RemoteReplicationPolicy,
    state_path: PathBuf,
    configured_state_directory: PathBuf,
    state_directory: PathBuf,
    state_directory_handle: Arc<File>,
    configured_lock_path: PathBuf,
    lock: File,
    in_process_lock: Mutex<()>,
}

impl RemoteReplicator {
    /// Opens durable replication state and validates the configured Git remote.
    ///
    /// # Errors
    ///
    /// Returns an error when repository state, the remote, or the destination
    /// branch is invalid.
    pub fn open(
        repository: GitRepository,
        policy: RemoteReplicationPolicy,
    ) -> Result<Self, RemoteReplicationError> {
        let _writer = repository
            .lock_writer()
            .map_err(RemoteReplicationError::repository)?;
        run_git_for_read(
            None,
            Some(repository.git_directory()),
            ["check-ref-format", &format!("refs/heads/{}", policy.branch)],
            None,
        )
        .map_err(|_| RemoteReplicationError::InvalidPolicy("branch"))?;
        let deadline = Instant::now()
            .checked_add(policy.timeout)
            .ok_or(RemoteReplicationError::InvalidPolicy("timeout"))?;
        configured_remote_snapshot(&repository, &policy, deadline, &|| false)?;
        let configured_state_directory = fs::canonicalize(repository.repository_state_directory())
            .map_err(RemoteReplicationError::Io)?;
        let (state_directory_handle, state_directory) =
            open_stable_directory(&configured_state_directory)
                .map_err(RemoteReplicationError::repository)?;
        let state_path = state_directory.join(STATE_FILE_NAME);
        let lock_path = state_directory.join(LOCK_FILE_NAME);
        let lock = open_lock_file(&lock_path).map_err(RemoteReplicationError::Io)?;
        let configured_lock_path = configured_state_directory.join(LOCK_FILE_NAME);
        validate_lock_file(&configured_lock_path, &lock)?;
        lock.sync_all().map_err(RemoteReplicationError::Io)?;
        Ok(Self {
            repository,
            policy,
            state_path,
            configured_state_directory,
            state_directory,
            state_directory_handle,
            configured_lock_path,
            lock,
            in_process_lock: Mutex::new(()),
        })
    }

    /// Attempts at most one push or reports the durable backoff deadline.
    ///
    /// Remote failures are returned as [`RemoteReplicationOutcome::Failed`]
    /// after the next retry has been persisted. Local validation and state I/O
    /// failures are returned as errors and never affect local publication.
    ///
    /// # Errors
    ///
    /// Returns an error when local repository or durable state validation fails.
    pub fn replicate(
        &self,
        now: OffsetDateTime,
    ) -> Result<RemoteReplicationOutcome, RemoteReplicationError> {
        self.replicate_controlled(now, &OffsetDateTime::now_utc, &|| false)
    }

    /// Runs one replication step that may cancel an in-flight Git subprocess.
    ///
    /// # Errors
    ///
    /// Returns an error when local repository or durable state validation fails.
    pub fn replicate_interruptible(
        &self,
        now: OffsetDateTime,
        cancelled: &impl Fn() -> bool,
    ) -> Result<RemoteReplicationOutcome, RemoteReplicationError> {
        self.replicate_controlled(now, &OffsetDateTime::now_utc, cancelled)
    }

    fn replicate_controlled(
        &self,
        now: OffsetDateTime,
        completed_at: &impl Fn() -> OffsetDateTime,
        cancelled: &impl Fn() -> bool,
    ) -> Result<RemoteReplicationOutcome, RemoteReplicationError> {
        let _in_process = match self.in_process_lock.try_lock() {
            Ok(guard) => guard,
            Err(MutexTryLockError::WouldBlock) => return Err(RemoteReplicationError::Busy),
            Err(MutexTryLockError::Poisoned(_)) => {
                return Err(RemoteReplicationError::LockPoisoned);
            }
        };
        validate_lock_file(&self.configured_lock_path, &self.lock)?;
        match self.lock.try_lock() {
            Ok(()) => {}
            Err(FileTryLockError::WouldBlock) => return Err(RemoteReplicationError::Busy),
            Err(FileTryLockError::Error(error)) => return Err(RemoteReplicationError::Io(error)),
        }
        let result = validate_lock_file(&self.configured_lock_path, &self.lock)
            .and_then(|()| self.replicate_locked(now, completed_at, cancelled));
        let unlock = self.lock.unlock().map_err(RemoteReplicationError::Io);
        finish_replication(result, unlock)
    }

    fn replicate_locked(
        &self,
        now: OffsetDateTime,
        completed_at: &impl Fn() -> OffsetDateTime,
        cancelled: &impl Fn() -> bool,
    ) -> Result<RemoteReplicationOutcome, RemoteReplicationError> {
        let deadline = Instant::now()
            .checked_add(self.policy.timeout)
            .ok_or(RemoteReplicationError::InvalidPolicy("timeout"))?;
        validate_pinned_directory(
            &self.configured_state_directory,
            &self.state_directory_handle,
        )
        .map_err(RemoteReplicationError::repository)?;
        let target = self
            .repository
            .resolve_commit_controlled(self.repository.official_ref(), deadline, cancelled)
            .map_err(RemoteReplicationError::repository)?;
        let remote =
            configured_remote_snapshot(&self.repository, &self.policy, deadline, cancelled)?;
        let remote_fingerprint = remote.fingerprint;
        let mut state = read_state(&self.state_path)?.unwrap_or_else(|| {
            ReplicationState::new(
                self.policy.remote(),
                self.policy.branch(),
                remote_fingerprint,
            )
        });
        validate_state(&state)?;
        if state.remote != self.policy.remote
            || state.branch != self.policy.branch
            || state.remote_fingerprint != remote_fingerprint
        {
            state = ReplicationState::new(
                self.policy.remote(),
                self.policy.branch(),
                remote_fingerprint,
            );
        }
        if state.replicated_commit.as_deref() == Some(target.as_str()) {
            return Ok(RemoteReplicationOutcome::UpToDate { commit: target });
        }
        if let Some(retry_at) = state.retry_at
            && now < retry_at
        {
            return Ok(RemoteReplicationOutcome::Deferred {
                commit: target,
                consecutive_failures: state.consecutive_failures,
                retry_at,
            });
        }

        let refspec = format!("{target}:refs/heads/{}", self.policy.branch);
        let push_repository =
            PushRepository::create(&self.repository, remote.object_format, deadline, cancelled)?;
        let push = push_repository.push(&remote.url, &refspec, deadline, cancelled);
        match push {
            Ok(_) => {
                state.replicated_commit = Some(target.clone());
                state.consecutive_failures = 0;
                state.retry_at = None;
                write_state(&self.state_directory, &self.state_path, &state)?;
                Ok(RemoteReplicationOutcome::Pushed { commit: target })
            }
            Err(
                GitTransactionError::GitCommand { .. } | GitTransactionError::GitDeadlineExceeded,
            ) => {
                state.consecutive_failures = state.consecutive_failures.saturating_add(1);
                let delay = retry_delay(&self.policy, state.consecutive_failures);
                let delay = time::Duration::try_from(delay)
                    .map_err(|_| RemoteReplicationError::InvalidPolicy("backoff"))?;
                let retry_at = completed_at()
                    .checked_add(delay)
                    .ok_or(RemoteReplicationError::InvalidPolicy("backoff"))?;
                state.retry_at = Some(retry_at);
                write_state(&self.state_directory, &self.state_path, &state)?;
                Ok(RemoteReplicationOutcome::Failed {
                    commit: target,
                    consecutive_failures: state.consecutive_failures,
                    retry_at,
                })
            }
            Err(GitTransactionError::GitCancelled) => Ok(RemoteReplicationOutcome::Cancelled),
            Err(error) => Err(RemoteReplicationError::repository(error)),
        }
    }
}

fn finish_replication(
    result: Result<RemoteReplicationOutcome, RemoteReplicationError>,
    unlock: Result<(), RemoteReplicationError>,
) -> Result<RemoteReplicationOutcome, RemoteReplicationError> {
    match (result, unlock) {
        (Ok(outcome), Ok(())) => Ok(outcome),
        (Err(RemoteReplicationError::Repository(error)), Err(unlock_error))
            if matches!(*error, GitTransactionError::GitCancelled) =>
        {
            Err(unlock_error)
        }
        (Err(RemoteReplicationError::Repository(error)), Ok(()))
            if matches!(*error, GitTransactionError::GitCancelled) =>
        {
            Ok(RemoteReplicationOutcome::Cancelled)
        }
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReplicationState {
    schema_version: u16,
    remote: String,
    branch: String,
    remote_fingerprint: Revision,
    replicated_commit: Option<String>,
    consecutive_failures: u32,
    #[serde(with = "time::serde::rfc3339::option")]
    retry_at: Option<OffsetDateTime>,
}

impl ReplicationState {
    fn new(remote: &str, branch: &str, remote_fingerprint: Revision) -> Self {
        Self {
            schema_version: REPLICATION_STATE_VERSION,
            remote: remote.into(),
            branch: branch.into(),
            remote_fingerprint,
            replicated_commit: None,
            consecutive_failures: 0,
            retry_at: None,
        }
    }
}

fn retry_delay(policy: &RemoteReplicationPolicy, failures: u32) -> Duration {
    let exponent = failures.saturating_sub(1).min(31);
    policy
        .initial_backoff
        .checked_mul(1_u32 << exponent)
        .unwrap_or(policy.maximum_backoff)
        .min(policy.maximum_backoff)
}

fn valid_remote_name(remote: &str) -> bool {
    !remote.is_empty()
        && !remote.starts_with(['-', '.'])
        && !remote.ends_with('.')
        && remote
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn valid_branch_name(branch: &str) -> bool {
    !branch.is_empty()
        && !branch.starts_with(['-', '.', '/'])
        && !branch.ends_with(['.', '/'])
        && !branch.ends_with(".lock")
        && !branch.contains("..")
        && !branch.contains("//")
        && !branch.contains("@{")
        && branch.bytes().all(|byte| {
            !byte.is_ascii_control()
                && !matches!(byte, b' ' | b'~' | b'^' | b':' | b'?' | b'*' | b'[' | b'\\')
        })
}

fn valid_object_id(value: &str) -> bool {
    matches!(value.len(), 40 | 64)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn fingerprint_remote_urls(urls: &[u8]) -> Result<Revision, RemoteReplicationError> {
    if urls.is_empty() {
        return Err(RemoteReplicationError::RemoteUnavailable);
    }
    Ok(Revision::from_bytes(Sha256::digest(urls).into()))
}

struct ConfiguredRemote {
    fingerprint: Revision,
    url: OsString,
    object_format: &'static str,
}

fn configured_remote_snapshot(
    repository: &GitRepository,
    policy: &RemoteReplicationPolicy,
    deadline: Instant,
    cancelled: &impl Fn() -> bool,
) -> Result<ConfiguredRemote, RemoteReplicationError> {
    repository
        .validate_replication_read(deadline, cancelled)
        .map_err(RemoteReplicationError::repository)?;
    let mirror_key = format!("remote.{}.mirror", policy.remote());
    let mirror = run_git_for_read_controlled(
        None,
        Some(repository.git_directory()),
        [
            "config",
            "--local",
            "--type=bool",
            "--default=false",
            "--get",
            mirror_key.as_str(),
        ],
        Some(deadline),
        cancelled,
    )
    .map_err(RemoteReplicationError::repository)?;
    if mirror.stdout != b"false\n" {
        return Err(RemoteReplicationError::InvalidPolicy("remote.mirror"));
    }
    let urls = run_git_for_read_controlled(
        None,
        Some(repository.git_directory()),
        ["remote", "get-url", "--push", "--all", policy.remote()],
        Some(deadline),
        cancelled,
    )
    .map_err(remote_lookup_error)?;
    let fingerprint = fingerprint_remote_urls(&urls.stdout)?;
    let url = parse_single_push_url(&urls.stdout)?;
    let object_format = run_git_until_controlled(
        None,
        Some(repository.git_directory()),
        ["rev-parse", "--show-object-format"],
        deadline,
        cancelled,
    )
    .map_err(RemoteReplicationError::repository)?;
    let object_format = parse_object_format(&object_format.stdout)?;
    Ok(ConfiguredRemote {
        fingerprint,
        url,
        object_format,
    })
}

fn remote_lookup_error(error: GitTransactionError) -> RemoteReplicationError {
    match error {
        GitTransactionError::GitCommand { .. } => RemoteReplicationError::RemoteUnavailable,
        error => RemoteReplicationError::repository(error),
    }
}

fn parse_single_push_url(output: &[u8]) -> Result<OsString, RemoteReplicationError> {
    let url = output
        .strip_suffix(b"\n")
        .ok_or(RemoteReplicationError::InvalidPolicy("remote.pushurl"))?;
    if url.is_empty() || url.iter().any(u8::is_ascii_control) {
        return Err(RemoteReplicationError::InvalidPolicy("remote.pushurl"));
    }
    #[cfg(unix)]
    {
        Ok(OsString::from_vec(url.to_vec()))
    }
    #[cfg(not(unix))]
    {
        String::from_utf8(url.to_vec())
            .map(OsString::from)
            .map_err(|_| RemoteReplicationError::InvalidPolicy("remote.pushurl"))
    }
}

fn parse_object_format(output: &[u8]) -> Result<&'static str, RemoteReplicationError> {
    match output {
        b"sha1\n" => Ok("sha1"),
        b"sha256\n" => Ok("sha256"),
        _ => Err(RemoteReplicationError::Repository(Box::new(
            GitTransactionError::InvalidGitOutput,
        ))),
    }
}

struct PushRepository {
    path: PathBuf,
}

impl PushRepository {
    fn create(
        repository: &GitRepository,
        object_format: &str,
        deadline: Instant,
        cancelled: &impl Fn() -> bool,
    ) -> Result<Self, RemoteReplicationError> {
        let path = std::env::temp_dir().join(format!(
            "agent-knowledge-push-{}-{}.git",
            std::process::id(),
            Ulid::generate()
        ));
        fs::create_dir(&path).map_err(RemoteReplicationError::Io)?;
        let snapshot = Self { path };
        let object_format = format!("--object-format={object_format}");
        run_git_until_controlled(
            None,
            None,
            [
                OsStr::new("init"),
                OsStr::new("--bare"),
                OsStr::new(&object_format),
                snapshot.path.as_os_str(),
            ],
            deadline,
            cancelled,
        )
        .map_err(RemoteReplicationError::repository)?;
        let mut alternate = repository
            .git_directory()
            .join("objects")
            .as_os_str()
            .as_encoded_bytes()
            .to_vec();
        if alternate.contains(&b'\n') {
            return Err(RemoteReplicationError::Repository(Box::new(
                GitTransactionError::InvalidGitOutput,
            )));
        }
        alternate.push(b'\n');
        fs::write(snapshot.path.join("objects/info/alternates"), alternate)
            .map_err(RemoteReplicationError::Io)?;
        Ok(snapshot)
    }

    fn push(
        &self,
        url: &OsStr,
        refspec: &str,
        deadline: Instant,
        cancelled: &impl Fn() -> bool,
    ) -> Result<(), GitTransactionError> {
        let config = format!("--config-env=remote.{SNAPSHOT_REMOTE}.url={PUSH_URL_ENVIRONMENT}");
        run_git_until_controlled_with_environment(
            None,
            Some(&self.path),
            [
                OsStr::new(&config),
                OsStr::new("push"),
                OsStr::new("--porcelain"),
                OsStr::new("--"),
                OsStr::new(SNAPSHOT_REMOTE),
                OsStr::new(refspec),
            ],
            deadline,
            cancelled,
            Some((OsStr::new(PUSH_URL_ENVIRONMENT), url)),
        )?;
        Ok(())
    }
}

impl Drop for PushRepository {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn validate_state(state: &ReplicationState) -> Result<(), RemoteReplicationError> {
    if state.schema_version != REPLICATION_STATE_VERSION
        || !valid_remote_name(&state.remote)
        || !valid_branch_name(&state.branch)
        || state
            .replicated_commit
            .as_deref()
            .is_some_and(|commit| !valid_object_id(commit))
        || (state.consecutive_failures == 0) != state.retry_at.is_none()
    {
        return Err(RemoteReplicationError::InvalidState);
    }
    Ok(())
}

fn read_state(path: &Path) -> Result<Option<ReplicationState>, RemoteReplicationError> {
    let file = match open_read_only_file(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(RemoteReplicationError::Io(error)),
    };
    let metadata = file.metadata().map_err(RemoteReplicationError::Io)?;
    if !metadata.file_type().is_file() || metadata.len() > MAXIMUM_STATE_BYTES {
        return Err(RemoteReplicationError::InvalidState);
    }
    let mut bytes = Vec::with_capacity(
        usize::try_from(metadata.len()).map_err(|_| RemoteReplicationError::InvalidState)?,
    );
    file.take(MAXIMUM_STATE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(RemoteReplicationError::Io)?;
    if bytes.len() as u64 > MAXIMUM_STATE_BYTES {
        return Err(RemoteReplicationError::InvalidState);
    }
    let state = serde_json::from_slice(&bytes).map_err(|_| RemoteReplicationError::InvalidState)?;
    validate_state(&state)?;
    Ok(Some(state))
}

fn open_lock_file(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    configure_no_follow(&mut options);
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "replication lock must be a regular file",
        ));
    }
    Ok(file)
}

fn validate_lock_file(path: &Path, pinned: &File) -> Result<(), RemoteReplicationError> {
    let configured = fs::symlink_metadata(path).map_err(RemoteReplicationError::Io)?;
    let pinned = pinned.metadata().map_err(RemoteReplicationError::Io)?;
    if !configured.file_type().is_file() || !same_metadata(&configured, &pinned) {
        return Err(RemoteReplicationError::LockReplaced);
    }
    Ok(())
}

fn open_read_only_file(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    configure_no_follow(&mut options);
    options.open(path)
}

fn configure_no_follow(options: &mut OpenOptions) {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK | nix::libc::O_CLOEXEC);
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = options;
    }
}

fn write_state(
    directory: &Path,
    path: &Path,
    state: &ReplicationState,
) -> Result<(), RemoteReplicationError> {
    validate_state(state)?;
    let mut bytes = serde_json::to_vec(state).map_err(|_| RemoteReplicationError::InvalidState)?;
    bytes.push(b'\n');
    if bytes.len() as u64 > MAXIMUM_STATE_BYTES {
        return Err(RemoteReplicationError::InvalidState);
    }
    let temporary = directory.join(TEMPORARY_STATE_FILE_NAME);
    remove_temporary_state(&temporary)?;
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        File::open(directory)?.sync_all()
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result.map_err(RemoteReplicationError::Io)
}

fn remove_temporary_state(path: &Path) -> Result<(), RemoteReplicationError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => Err(RemoteReplicationError::InvalidState),
        Ok(_) => fs::remove_file(path).map_err(RemoteReplicationError::Io),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(RemoteReplicationError::Io(error)),
    }
}

/// Failure to validate or persist one remote-replication step.
#[derive(Debug)]
pub enum RemoteReplicationError {
    /// A policy field was unsafe or outside supported bounds.
    InvalidPolicy(&'static str),
    /// The configured remote was absent or unreadable.
    RemoteUnavailable,
    /// Another replication step is already active.
    Busy,
    /// An earlier in-process replication attempt panicked while holding its lock.
    LockPoisoned,
    /// The fixed cross-process lock entry no longer names the pinned file.
    LockReplaced,
    /// Durable replication state was malformed or unsafe.
    InvalidState,
    /// Local repository inspection failed.
    Repository(Box<GitTransactionError>),
    /// Durable state or lock I/O failed.
    Io(io::Error),
}

impl RemoteReplicationError {
    fn repository(error: GitTransactionError) -> Self {
        Self::Repository(Box::new(error))
    }
}

impl fmt::Display for RemoteReplicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPolicy(field) => {
                write!(formatter, "remote replication `{field}` is invalid")
            }
            Self::RemoteUnavailable => formatter.write_str("configured Git remote is unavailable"),
            Self::Busy => formatter.write_str("another remote replication attempt is active"),
            Self::LockPoisoned => formatter.write_str("remote replication lock is poisoned"),
            Self::LockReplaced => formatter.write_str("remote replication lock was replaced"),
            Self::InvalidState => {
                formatter.write_str("durable remote replication state is invalid")
            }
            Self::Repository(error) => {
                write!(formatter, "local repository validation failed: {error}")
            }
            Self::Io(error) => write!(formatter, "remote replication state I/O failed: {error}"),
        }
    }
}

impl std::error::Error for RemoteReplicationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Repository(error) => Some(error),
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::time::{Duration, Instant};

    use time::OffsetDateTime;
    use ulid::Ulid;

    use super::{
        GitTransactionError, LOCK_FILE_NAME, PushRepository, RemoteReplicationError,
        RemoteReplicationOutcome, RemoteReplicationPolicy, RemoteReplicator, STATE_FILE_NAME,
        configured_remote_snapshot, finish_replication,
    };
    use crate::{GitIdentity, GitRepository};

    struct Fixture {
        root: PathBuf,
        repository: PathBuf,
        canonical: PathBuf,
        work: PathBuf,
        remote: PathBuf,
    }

    impl Fixture {
        fn create() -> Self {
            let root = std::env::temp_dir().join(format!(
                "agent-knowledge-replication-test-{}",
                Ulid::generate()
            ));
            fs::create_dir(&root)
                .unwrap_or_else(|error| panic!("test root must be created: {error}"));
            let repository = root.join("repository");
            let canonical = root.join("content");
            let work = root.join("work");
            let remote = root.join("remote");
            let seed = root.join("seed");
            git(
                None,
                ["init", "--bare", "--initial-branch=main"],
                Some(&repository),
            );
            git(
                None,
                ["init", "--bare", "--initial-branch=main"],
                Some(&remote),
            );
            git(None, ["init", "--initial-branch=main"], Some(&seed));
            git(
                Some(&seed),
                [
                    "-c",
                    "user.name=Fictional Test Author",
                    "-c",
                    "user.email=worker@example.invalid",
                    "commit",
                    "--allow-empty",
                    "-m",
                    "Initialize fictional knowledge",
                ],
                None,
            );
            git(Some(&seed), ["remote", "add", "origin"], Some(&repository));
            git(Some(&seed), ["push", "origin", "main"], None);
            git(
                None,
                [
                    &format!("--git-dir={}", repository.display()),
                    "remote",
                    "add",
                    "fictional-backup",
                ],
                Some(&remote),
            );
            let git_directory = format!("--git-dir={}", repository.display());
            let canonical_path = canonical.display().to_string();
            git(
                None,
                [
                    git_directory.as_str(),
                    "worktree",
                    "add",
                    canonical_path.as_str(),
                    "main",
                ],
                None,
            );
            fs::create_dir(&work)
                .unwrap_or_else(|error| panic!("work root must be created: {error}"));
            Self {
                root,
                repository,
                canonical,
                work,
                remote,
            }
        }

        fn repository(&self) -> GitRepository {
            let identity = GitIdentity::new("Fictional Knowledge Worker", "worker@example.invalid")
                .unwrap_or_else(|error| panic!("fixture identity must be valid: {error}"));
            GitRepository::open(
                &self.repository,
                &self.canonical,
                &self.work,
                "main",
                identity,
            )
            .unwrap_or_else(|error| panic!("fixture repository must open: {error}"))
        }

        fn policy(&self) -> RemoteReplicationPolicy {
            RemoteReplicationPolicy::new(
                "fictional-backup",
                "main",
                Duration::from_secs(5),
                Duration::from_secs(10),
                Duration::from_secs(40),
            )
            .unwrap_or_else(|error| panic!("fixture policy must be valid: {error}"))
        }

        fn local_commit(&self) -> String {
            git_output(&self.repository, "refs/heads/main")
        }

        fn remote_commit(&self) -> String {
            git_output(&self.remote, "refs/heads/main")
        }

        fn advance(&self) {
            git(
                Some(&self.canonical),
                [
                    "-c",
                    "user.name=Fictional Test Author",
                    "-c",
                    "user.email=worker@example.invalid",
                    "commit",
                    "--allow-empty",
                    "-m",
                    "Advance fictional knowledge",
                ],
                None,
            );
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            if let Err(error) = fs::remove_dir_all(&self.root)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                panic!("test root must be removed: {error}");
            }
        }
    }

    #[test]
    fn rejects_a_mirror_remote_before_replication_starts() {
        let fixture = Fixture::create();
        let repository = fixture.repository();
        let git_directory = format!("--git-dir={}", fixture.repository.display());
        git(
            None,
            [
                git_directory.as_str(),
                "config",
                "remote.fictional-backup.mirror",
                "true",
            ],
            None,
        );

        assert!(matches!(
            RemoteReplicator::open(repository, fixture.policy()),
            Err(RemoteReplicationError::InvalidPolicy("remote.mirror"))
        ));
    }

    #[test]
    fn rejects_multiple_push_destinations() {
        let fixture = Fixture::create();
        let repository = fixture.repository();
        let git_directory = format!("--git-dir={}", fixture.repository.display());
        let second = fixture.root.join("second-remote");
        for url in [&fixture.remote, &second] {
            git(
                None,
                [
                    git_directory.as_str(),
                    "config",
                    "--add",
                    "remote.fictional-backup.pushurl",
                ],
                Some(url),
            );
        }

        assert!(matches!(
            RemoteReplicator::open(repository, fixture.policy()),
            Err(RemoteReplicationError::InvalidPolicy("remote.pushurl"))
        ));
    }

    #[test]
    fn rejects_a_remote_changed_to_mirror_mode_before_a_push() {
        let fixture = Fixture::create();
        let repository = fixture.repository();
        let replicator = RemoteReplicator::open(repository, fixture.policy())
            .unwrap_or_else(|error| panic!("replicator must open: {error}"));
        let git_directory = format!("--git-dir={}", fixture.repository.display());
        git(
            None,
            [
                git_directory.as_str(),
                "config",
                "remote.fictional-backup.mirror",
                "true",
            ],
            None,
        );

        assert!(matches!(
            replicator.replicate(OffsetDateTime::UNIX_EPOCH),
            Err(RemoteReplicationError::InvalidPolicy("remote.mirror"))
        ));
    }

    #[test]
    fn rejects_unsafe_local_git_configuration_added_after_open() {
        let fixture = Fixture::create();
        let repository = fixture.repository();
        let replicator = RemoteReplicator::open(repository, fixture.policy())
            .unwrap_or_else(|error| panic!("replicator must open: {error}"));
        let git_directory = format!("--git-dir={}", fixture.repository.display());
        git(
            None,
            [
                git_directory.as_str(),
                "config",
                "credential.helper",
                "fictional-helper",
            ],
            None,
        );

        assert!(matches!(
            replicator.replicate(OffsetDateTime::UNIX_EPOCH),
            Err(RemoteReplicationError::Repository(error))
                if matches!(*error, crate::GitTransactionError::UnsafeGitConfig)
        ));
    }

    #[test]
    fn push_uses_the_validated_destination_and_isolated_config_snapshot() {
        let fixture = Fixture::create();
        let repository = fixture.repository();
        let policy = fixture.policy();
        let deadline = Instant::now() + Duration::from_secs(5);
        let remote = configured_remote_snapshot(&repository, &policy, deadline, &|| false)
            .unwrap_or_else(|error| panic!("remote snapshot must be captured: {error}"));
        let replacement = fixture.root.join("replacement-remote");
        git(
            None,
            ["init", "--bare", "--initial-branch=main"],
            Some(&replacement),
        );
        let git_directory = format!("--git-dir={}", fixture.repository.display());
        git(
            None,
            [
                git_directory.as_str(),
                "remote",
                "set-url",
                "fictional-backup",
            ],
            Some(&replacement),
        );
        git(
            None,
            [
                git_directory.as_str(),
                "config",
                "credential.helper",
                "fictional-helper",
            ],
            None,
        );
        let push_repository =
            PushRepository::create(&repository, remote.object_format, deadline, &|| false)
                .unwrap_or_else(|error| {
                    panic!("isolated push repository must initialize: {error}")
                });
        let commit = fixture.local_commit();
        let refspec = format!("{commit}:refs/heads/main");

        push_repository
            .push(&remote.url, &refspec, deadline, &|| false)
            .unwrap_or_else(|error| panic!("captured destination push must succeed: {error}"));
        assert_eq!(fixture.remote_commit(), commit);
    }

    #[test]
    fn pushes_each_new_official_commit_and_records_success() {
        let fixture = Fixture::create();
        let replicator = RemoteReplicator::open(fixture.repository(), fixture.policy())
            .unwrap_or_else(|error| panic!("replicator must open: {error}"));
        let now = OffsetDateTime::UNIX_EPOCH;
        let initial = fixture.local_commit();

        assert_eq!(
            replicate(&replicator, now),
            RemoteReplicationOutcome::Pushed {
                commit: initial.clone()
            }
        );
        assert_eq!(fixture.remote_commit(), initial);
        assert_eq!(
            replicate(&replicator, now),
            RemoteReplicationOutcome::UpToDate {
                commit: initial.clone()
            }
        );

        fixture.advance();
        let advanced = fixture.local_commit();
        assert_ne!(advanced, initial);
        assert_eq!(
            replicate(&replicator, now),
            RemoteReplicationOutcome::Pushed {
                commit: advanced.clone()
            }
        );
        assert_eq!(fixture.remote_commit(), advanced);
    }

    #[test]
    fn foreground_writer_ownership_does_not_block_replication_reads() {
        let fixture = Fixture::create();
        let repository = fixture.repository();
        let replicator = RemoteReplicator::open(repository.clone(), fixture.policy())
            .unwrap_or_else(|error| panic!("replicator must open: {error}"));
        let _writer = repository
            .lock_writer()
            .unwrap_or_else(|error| panic!("foreground writer lock must be acquired: {error}"));

        assert!(matches!(
            replicate(&replicator, OffsetDateTime::UNIX_EPOCH),
            RemoteReplicationOutcome::Pushed { .. }
        ));
    }

    #[test]
    fn repointed_remote_invalidates_the_confirmed_commit_cache() {
        let fixture = Fixture::create();
        let repository = fixture.repository();
        let replicator = RemoteReplicator::open(repository, fixture.policy())
            .unwrap_or_else(|error| panic!("replicator must open: {error}"));
        let now = OffsetDateTime::UNIX_EPOCH;
        let commit = fixture.local_commit();
        assert!(matches!(
            replicate(&replicator, now),
            RemoteReplicationOutcome::Pushed { .. }
        ));

        let replacement = fixture.root.join("replacement-remote");
        git(
            None,
            ["init", "--bare", "--initial-branch=main"],
            Some(&replacement),
        );
        let git_directory = format!("--git-dir={}", fixture.repository.display());
        git(
            None,
            [
                git_directory.as_str(),
                "remote",
                "set-url",
                "fictional-backup",
            ],
            Some(&replacement),
        );

        assert_eq!(
            replicate(&replicator, now),
            RemoteReplicationOutcome::Pushed {
                commit: commit.clone()
            }
        );
        assert_eq!(git_output(&replacement, "refs/heads/main"), commit);
    }

    #[test]
    fn persists_backoff_across_restart_and_recovers_after_remote_returns() {
        let fixture = Fixture::create();
        let repository = fixture.repository();
        let policy = fixture.policy();
        let replicator = RemoteReplicator::open(repository.clone(), policy.clone())
            .unwrap_or_else(|error| panic!("replicator must open: {error}"));
        fs::remove_dir_all(&fixture.remote)
            .unwrap_or_else(|error| panic!("remote outage must be simulated: {error}"));
        let now = OffsetDateTime::UNIX_EPOCH;
        let commit = fixture.local_commit();

        assert_eq!(
            replicate_completed_at(&replicator, now, now + time::Duration::seconds(30)),
            RemoteReplicationOutcome::Failed {
                commit: commit.clone(),
                consecutive_failures: 1,
                retry_at: now + time::Duration::seconds(40),
            }
        );
        assert_eq!(
            replicate(&replicator, now + time::Duration::seconds(40)),
            RemoteReplicationOutcome::Failed {
                commit: commit.clone(),
                consecutive_failures: 2,
                retry_at: now + time::Duration::seconds(60),
            }
        );
        drop(replicator);
        git(
            None,
            ["init", "--bare", "--initial-branch=main"],
            Some(&fixture.remote),
        );
        let restarted = RemoteReplicator::open(repository, policy)
            .unwrap_or_else(|error| panic!("restarted replicator must open: {error}"));
        assert_eq!(
            replicate(&restarted, now + time::Duration::seconds(59)),
            RemoteReplicationOutcome::Deferred {
                commit: commit.clone(),
                consecutive_failures: 2,
                retry_at: now + time::Duration::seconds(60),
            }
        );
        assert_eq!(
            replicate(&restarted, now + time::Duration::seconds(60)),
            RemoteReplicationOutcome::Pushed {
                commit: commit.clone()
            }
        );
        assert_eq!(fixture.remote_commit(), commit);
    }

    #[test]
    fn rejects_corrupted_durable_state_without_pushing() {
        let fixture = Fixture::create();
        let repository = fixture.repository();
        let replicator = RemoteReplicator::open(repository, fixture.policy())
            .unwrap_or_else(|error| panic!("replicator must open: {error}"));
        let state = fixture
            .repository
            .join("agent-knowledge")
            .join(STATE_FILE_NAME);
        fs::write(state, b"{\"schema_version\":1}\n")
            .unwrap_or_else(|error| panic!("corrupt fixture state must be written: {error}"));

        assert!(matches!(
            replicator.replicate(OffsetDateTime::UNIX_EPOCH),
            Err(RemoteReplicationError::InvalidState)
        ));
    }

    #[test]
    fn validates_corrupted_state_before_resetting_a_changed_destination() {
        let fixture = Fixture::create();
        let repository = fixture.repository();
        let replicator = RemoteReplicator::open(repository, fixture.policy())
            .unwrap_or_else(|error| panic!("replicator must open: {error}"));
        let state = fixture
            .repository
            .join("agent-knowledge")
            .join(STATE_FILE_NAME);
        let invalid = format!(
            "{{\"schema_version\":1,\"remote\":\"other\",\"branch\":\"main\",\"remote_fingerprint\":\"sha256:{}\",\"replicated_commit\":\"invalid\",\"consecutive_failures\":0,\"retry_at\":null}}\n",
            "00".repeat(32)
        );
        fs::write(state, invalid)
            .unwrap_or_else(|error| panic!("invalid fixture state must be written: {error}"));

        assert!(matches!(
            replicator.replicate(OffsetDateTime::UNIX_EPOCH),
            Err(RemoteReplicationError::InvalidState)
        ));
    }

    #[test]
    fn rejects_a_replaced_cross_process_lock() {
        let fixture = Fixture::create();
        let repository = fixture.repository();
        let replicator = RemoteReplicator::open(repository, fixture.policy())
            .unwrap_or_else(|error| panic!("replicator must open: {error}"));
        let lock = fixture
            .repository
            .join("agent-knowledge")
            .join(LOCK_FILE_NAME);
        fs::rename(&lock, lock.with_extension("replaced"))
            .unwrap_or_else(|error| panic!("fixture lock must be moved: {error}"));
        fs::write(&lock, b"")
            .unwrap_or_else(|error| panic!("replacement lock must be written: {error}"));

        assert!(matches!(
            replicator.replicate(OffsetDateTime::UNIX_EPOCH),
            Err(RemoteReplicationError::LockReplaced)
        ));
    }

    #[test]
    fn cancellation_does_not_create_retry_state() {
        let fixture = Fixture::create();
        let repository = fixture.repository();
        let replicator = RemoteReplicator::open(repository, fixture.policy())
            .unwrap_or_else(|error| panic!("replicator must open: {error}"));
        let outcome = replicator.replicate_controlled(
            OffsetDateTime::UNIX_EPOCH,
            &|| OffsetDateTime::UNIX_EPOCH,
            &|| true,
        );

        assert!(matches!(outcome, Ok(RemoteReplicationOutcome::Cancelled)));
        assert!(
            !fixture
                .repository
                .join("agent-knowledge")
                .join(STATE_FILE_NAME)
                .exists()
        );
    }

    #[test]
    fn unlock_failure_takes_precedence_over_pre_push_cancellation() {
        let cancellation = Err(RemoteReplicationError::Repository(Box::new(
            GitTransactionError::GitCancelled,
        )));
        let unlock = Err(RemoteReplicationError::Io(std::io::Error::other(
            "fictional unlock failure",
        )));

        assert!(matches!(
            finish_replication(cancellation, unlock),
            Err(RemoteReplicationError::Io(error))
                if error.to_string() == "fictional unlock failure"
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn cancellation_covers_a_blocked_pre_push_config_inspection() {
        use nix::sys::stat::Mode;
        use nix::unistd::mkfifo;

        let fixture = Fixture::create();
        let repository = fixture.repository();
        let replicator = RemoteReplicator::open(repository, fixture.policy())
            .unwrap_or_else(|error| panic!("replicator must open: {error}"));
        let config = fixture.repository.join("config");
        fs::rename(&config, fixture.repository.join("config.saved"))
            .unwrap_or_else(|error| panic!("repository config must be moved: {error}"));
        mkfifo(&config, Mode::S_IRUSR | Mode::S_IWUSR)
            .unwrap_or_else(|error| panic!("blocking config fixture must be created: {error}"));
        let started = Instant::now();

        let outcome = replicator.replicate_interruptible(OffsetDateTime::UNIX_EPOCH, &|| {
            started.elapsed() >= Duration::from_millis(50)
        });

        assert!(matches!(outcome, Ok(RemoteReplicationOutcome::Cancelled)));
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn retry_deadline_overflow_is_reported_without_panicking() {
        let fixture = Fixture::create();
        let repository = fixture.repository();
        let replicator = RemoteReplicator::open(repository, fixture.policy())
            .unwrap_or_else(|error| panic!("replicator must open: {error}"));
        fs::remove_dir_all(&fixture.remote)
            .unwrap_or_else(|error| panic!("fixture remote must be removed: {error}"));
        let maximum = OffsetDateTime::parse(
            "9999-12-31T23:59:59Z",
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap_or_else(|error| panic!("maximum fixture timestamp must parse: {error}"));

        assert!(matches!(
            replicator.replicate_controlled(maximum, &|| maximum, &|| false),
            Err(RemoteReplicationError::InvalidPolicy("backoff"))
        ));
    }

    fn git<const N: usize>(
        working_directory: Option<&Path>,
        arguments: [&str; N],
        path_argument: Option<&Path>,
    ) {
        let mut command = Command::new("git");
        if let Some(working_directory) = working_directory {
            command.current_dir(working_directory);
        }
        command.args(arguments);
        if let Some(path_argument) = path_argument {
            command.arg(path_argument);
        }
        let status = command
            .status()
            .unwrap_or_else(|error| panic!("Git fixture command must run: {error}"));
        assert!(status.success(), "Git fixture command failed with {status}");
    }

    fn replicate(replicator: &RemoteReplicator, now: OffsetDateTime) -> RemoteReplicationOutcome {
        replicate_completed_at(replicator, now, now)
    }

    fn replicate_completed_at(
        replicator: &RemoteReplicator,
        now: OffsetDateTime,
        completed_at: OffsetDateTime,
    ) -> RemoteReplicationOutcome {
        replicator
            .replicate_controlled(now, &|| completed_at, &|| false)
            .unwrap_or_else(|error| panic!("replication step must complete: {error}"))
    }

    fn git_output(repository: &Path, revision: &str) -> String {
        let output = Command::new("git")
            .arg(format!("--git-dir={}", repository.display()))
            .args(["rev-parse", revision])
            .output()
            .unwrap_or_else(|error| panic!("Git fixture command must run: {error}"));
        assert!(output.status.success(), "Git fixture revision must exist");
        String::from_utf8(output.stdout)
            .unwrap_or_else(|error| panic!("Git fixture output must be UTF-8: {error}"))
            .trim()
            .into()
    }
}

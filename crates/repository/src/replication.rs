use std::fmt;
use std::fs::{self, File, OpenOptions, TryLockError as FileTryLockError};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, TryLockError as MutexTryLockError};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use ulid::Ulid;

use crate::git::{
    GitRepository, GitTransactionError, open_stable_directory, run_git_for_read,
    validate_pinned_directory,
};

const REPLICATION_STATE_VERSION: u16 = 1;
const MAXIMUM_STATE_BYTES: u64 = 64 * 1024;
const STATE_FILE_NAME: &str = "remote-replication-v1.json";
const LOCK_FILE_NAME: &str = "remote-replication.lock";

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
        run_git_for_read(
            None,
            Some(repository.git_directory()),
            ["remote", "get-url", "--push", "--all", policy.remote()],
            None,
        )
        .map_err(|_| RemoteReplicationError::RemoteUnavailable)?;
        let configured_state_directory = fs::canonicalize(repository.repository_state_directory())
            .map_err(RemoteReplicationError::Io)?;
        let (state_directory_handle, state_directory) =
            open_stable_directory(&configured_state_directory)
                .map_err(RemoteReplicationError::repository)?;
        let state_path = state_directory.join(STATE_FILE_NAME);
        let lock_path = state_directory.join(LOCK_FILE_NAME);
        let lock = open_lock_file(&lock_path).map_err(RemoteReplicationError::Io)?;
        lock.sync_all().map_err(RemoteReplicationError::Io)?;
        Ok(Self {
            repository,
            policy,
            state_path,
            configured_state_directory,
            state_directory,
            state_directory_handle,
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
        let _in_process = match self.in_process_lock.try_lock() {
            Ok(guard) => guard,
            Err(MutexTryLockError::WouldBlock) => return Err(RemoteReplicationError::Busy),
            Err(MutexTryLockError::Poisoned(_)) => {
                return Err(RemoteReplicationError::LockPoisoned);
            }
        };
        match self.lock.try_lock() {
            Ok(()) => {}
            Err(FileTryLockError::WouldBlock) => return Err(RemoteReplicationError::Busy),
            Err(FileTryLockError::Error(error)) => return Err(RemoteReplicationError::Io(error)),
        }
        let result = self.replicate_locked(now);
        let unlock = self.lock.unlock().map_err(RemoteReplicationError::Io);
        match (result, unlock) {
            (Ok(outcome), Ok(())) => Ok(outcome),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
        }
    }

    fn replicate_locked(
        &self,
        now: OffsetDateTime,
    ) -> Result<RemoteReplicationOutcome, RemoteReplicationError> {
        validate_pinned_directory(
            &self.configured_state_directory,
            &self.state_directory_handle,
        )
        .map_err(RemoteReplicationError::repository)?;
        let target = {
            let _writer = self
                .repository
                .lock_writer()
                .map_err(RemoteReplicationError::repository)?;
            self.repository
                .resolve_commit(self.repository.official_ref())
                .map_err(RemoteReplicationError::repository)?
        };
        let mut state = read_state(&self.state_path)?
            .unwrap_or_else(|| ReplicationState::new(self.policy.remote(), self.policy.branch()));
        if state.remote != self.policy.remote || state.branch != self.policy.branch {
            state = ReplicationState::new(self.policy.remote(), self.policy.branch());
        }
        validate_state(&state)?;
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

        let deadline = Instant::now()
            .checked_add(self.policy.timeout)
            .ok_or(RemoteReplicationError::InvalidPolicy("timeout"))?;
        let refspec = format!("{target}:refs/heads/{}", self.policy.branch);
        let push = run_git_for_read(
            None,
            Some(self.repository.git_directory()),
            ["push", "--porcelain", "--", self.policy.remote(), &refspec],
            Some(deadline),
        );
        match push {
            Ok(_) => {
                state.replicated_commit = Some(target.clone());
                state.consecutive_failures = 0;
                state.retry_at = None;
                write_state(&self.state_directory, &self.state_path, &state)?;
                Ok(RemoteReplicationOutcome::Pushed { commit: target })
            }
            Err(_) => {
                state.consecutive_failures = state.consecutive_failures.saturating_add(1);
                let delay = retry_delay(&self.policy, state.consecutive_failures);
                let retry_at = now
                    + time::Duration::try_from(delay)
                        .map_err(|_| RemoteReplicationError::InvalidPolicy("backoff"))?;
                state.retry_at = Some(retry_at);
                write_state(&self.state_directory, &self.state_path, &state)?;
                Ok(RemoteReplicationOutcome::Failed {
                    commit: target,
                    consecutive_failures: state.consecutive_failures,
                    retry_at,
                })
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReplicationState {
    schema_version: u16,
    remote: String,
    branch: String,
    replicated_commit: Option<String>,
    consecutive_failures: u32,
    #[serde(with = "time::serde::rfc3339::option")]
    retry_at: Option<OffsetDateTime>,
}

impl ReplicationState {
    fn new(remote: &str, branch: &str) -> Self {
        Self {
            schema_version: REPLICATION_STATE_VERSION,
            remote: remote.into(),
            branch: branch.into(),
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
    let temporary = directory.join(format!(".remote-replication-{}.tmp", Ulid::generate()));
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
    use std::time::Duration;

    use time::OffsetDateTime;
    use ulid::Ulid;

    use super::{
        RemoteReplicationError, RemoteReplicationOutcome, RemoteReplicationPolicy,
        RemoteReplicator, STATE_FILE_NAME,
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
            replicate(&replicator, now),
            RemoteReplicationOutcome::Failed {
                commit: commit.clone(),
                consecutive_failures: 1,
                retry_at: now + time::Duration::seconds(10),
            }
        );
        assert_eq!(
            replicate(&replicator, now + time::Duration::seconds(10)),
            RemoteReplicationOutcome::Failed {
                commit: commit.clone(),
                consecutive_failures: 2,
                retry_at: now + time::Duration::seconds(30),
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
            replicate(&restarted, now + time::Duration::seconds(29)),
            RemoteReplicationOutcome::Deferred {
                commit: commit.clone(),
                consecutive_failures: 2,
                retry_at: now + time::Duration::seconds(30),
            }
        );
        assert_eq!(
            replicate(&restarted, now + time::Duration::seconds(30)),
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
        replicator
            .replicate(now)
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

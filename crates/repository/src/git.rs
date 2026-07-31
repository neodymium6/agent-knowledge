use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use agent_knowledge_core::{BatchId, ErrorCode, RequestId};
use agent_knowledge_queue::{ClaimToken, ClaimedPackage};

use crate::{ApplyError, ContentPolicy, apply_claimed};

const MAXIMUM_DIAGNOSTIC_BYTES: usize = 8 * 1024;

/// Fixed author identity for mechanically generated knowledge commits.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GitIdentity {
    name: String,
    email: String,
}

impl GitIdentity {
    /// Creates a commit identity after rejecting empty or control-containing values.
    ///
    /// # Errors
    ///
    /// Returns an error when either value is unsafe for Git configuration.
    pub fn new(name: &str, email: &str) -> Result<Self, GitTransactionError> {
        if !valid_identity_value(name) || !valid_identity_value(email) {
            return Err(GitTransactionError::InvalidIdentity);
        }
        Ok(Self {
            name: name.into(),
            email: email.into(),
        })
    }
}

fn valid_identity_value(value: &str) -> bool {
    !value.trim().is_empty() && !value.chars().any(char::is_control)
}

/// A request-specific deterministic failure isolated from the batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestFailure {
    token: ClaimToken,
    error_code: ErrorCode,
}

impl RequestFailure {
    /// Returns the exact queue ownership token.
    #[must_use]
    pub const fn token(self) -> ClaimToken {
        self.token
    }

    /// Returns the stable request failure code.
    #[must_use]
    pub const fn error_code(self) -> ErrorCode {
        self.error_code
    }
}

/// Result of one repository batch before Quartz publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BatchCommitOutcome {
    /// Every claim failed deterministically, so no commit was created.
    NoChanges {
        /// Isolated request failures.
        failures: Vec<RequestFailure>,
    },
    /// Successful requests were committed and the official branch advanced.
    Committed {
        /// New official Git commit.
        commit: String,
        /// Exact successful queue ownership tokens.
        successful: Vec<ClaimToken>,
        /// Isolated request failures.
        failures: Vec<RequestFailure>,
    },
}

/// Central bare repository and its canonical linked worktree.
#[derive(Clone, Debug)]
pub struct GitRepository {
    git_directory: PathBuf,
    canonical_worktree: PathBuf,
    work_root: PathBuf,
    official_ref: String,
    identity: GitIdentity,
}

impl GitRepository {
    /// Opens and validates a bare repository transaction boundary.
    ///
    /// # Errors
    ///
    /// Returns an error when paths, the official branch, or Git repository
    /// configuration are invalid.
    pub fn open(
        git_directory: &Path,
        canonical_worktree: &Path,
        work_root: &Path,
        official_branch: &str,
        identity: GitIdentity,
    ) -> Result<Self, GitTransactionError> {
        ensure_real_directory(git_directory)?;
        if official_branch.is_empty() || official_branch.chars().any(char::is_control) {
            return Err(GitTransactionError::InvalidOfficialBranch);
        }
        let official_ref = format!("refs/heads/{official_branch}");
        run_git(
            None,
            Some(git_directory),
            [OsStr::new("check-ref-format"), OsStr::new(&official_ref)],
        )?;
        let bare = run_git(
            None,
            Some(git_directory),
            [OsStr::new("rev-parse"), OsStr::new("--is-bare-repository")],
        )?;
        if parse_text(&bare.stdout)? != "true" {
            return Err(GitTransactionError::RepositoryNotBare);
        }
        ensure_real_directory(canonical_worktree)?;
        run_git(
            Some(canonical_worktree),
            None,
            [OsStr::new("rev-parse"), OsStr::new("--is-inside-work-tree")],
        )?;
        ensure_or_create_real_directory(work_root)?;
        let common_directory = run_git(
            Some(canonical_worktree),
            None,
            [
                OsStr::new("rev-parse"),
                OsStr::new("--path-format=absolute"),
                OsStr::new("--git-common-dir"),
            ],
        )?;
        let common_directory = PathBuf::from(parse_text(&common_directory.stdout)?);
        let configured_repository =
            fs::canonicalize(git_directory).map_err(GitTransactionError::Io)?;
        let common_directory =
            fs::canonicalize(common_directory).map_err(GitTransactionError::Io)?;
        if configured_repository != common_directory {
            return Err(GitTransactionError::CanonicalWorktreeMismatch);
        }
        let symbolic_head = run_git(
            Some(canonical_worktree),
            None,
            [OsStr::new("symbolic-ref"), OsStr::new("HEAD")],
        )?;
        if parse_text(&symbolic_head.stdout)? != official_ref {
            return Err(GitTransactionError::CanonicalWorktreeBranchMismatch);
        }
        validate_nonoverlapping_paths(git_directory, canonical_worktree, work_root)?;

        Ok(Self {
            git_directory: git_directory.into(),
            canonical_worktree: canonical_worktree.into(),
            work_root: work_root.into(),
            official_ref,
            identity,
        })
    }

    /// Applies a bounded ordered claim batch, creates one commit, and advances
    /// the official branch with an expected-old-commit check.
    ///
    /// Request-specific deterministic failures are rolled back to the last
    /// successful index tree and returned separately. Infrastructure or
    /// content-store failures abort the batch without advancing the official
    /// branch.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty batch, Git or filesystem failure,
    /// non-request content failure, or concurrent official-branch change.
    pub fn apply_batch(
        &self,
        batch_id: BatchId,
        claims: &[ClaimedPackage],
        policy: ContentPolicy,
    ) -> Result<BatchCommitOutcome, GitTransactionError> {
        self.apply_batch_with_hook(batch_id, claims, policy, |_, _| Ok(()))
    }

    fn apply_batch_with_hook<F>(
        &self,
        batch_id: BatchId,
        claims: &[ClaimedPackage],
        policy: ContentPolicy,
        before_publish: F,
    ) -> Result<BatchCommitOutcome, GitTransactionError>
    where
        F: FnOnce(&str, &str) -> Result<(), GitTransactionError>,
    {
        if claims.is_empty() {
            return Err(GitTransactionError::EmptyBatch);
        }
        let base = self.resolve_commit(&self.official_ref)?;
        let branch = format!("transactions/{batch_id}");
        let transaction_ref = format!("refs/heads/{branch}");
        let worktree = self.work_root.join(format!("batch-{batch_id}"));
        match fs::symlink_metadata(&worktree) {
            Ok(_) => {
                return Err(GitTransactionError::WorktreeAlreadyExists(worktree));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(GitTransactionError::Io(error)),
        }

        run_git(
            None,
            Some(&self.git_directory),
            [
                OsStr::new("worktree"),
                OsStr::new("add"),
                OsStr::new("-b"),
                OsStr::new(&branch),
                worktree.as_os_str(),
                OsStr::new(&base),
            ],
        )?;

        let result =
            self.apply_in_worktree(batch_id, claims, policy, &base, &worktree, before_publish);
        if matches!(result, Ok(BatchCommitOutcome::NoChanges { .. })) {
            self.remove_transaction(&worktree, &transaction_ref, &base)?;
        }
        result
    }

    fn apply_in_worktree<F>(
        &self,
        batch_id: BatchId,
        claims: &[ClaimedPackage],
        policy: ContentPolicy,
        base: &str,
        worktree: &Path,
        before_publish: F,
    ) -> Result<BatchCommitOutcome, GitTransactionError>
    where
        F: FnOnce(&str, &str) -> Result<(), GitTransactionError>,
    {
        let mut tree = resolve_in_worktree(worktree, "HEAD^{tree}")?;
        let mut successful = Vec::new();
        let mut failures = Vec::new();

        for claim in claims {
            match apply_claimed(worktree, claim, policy) {
                Ok(_) => {
                    run_git(
                        Some(worktree),
                        None,
                        [OsStr::new("add"), OsStr::new("--all")],
                    )?;
                    let staged_tree = run_git(Some(worktree), None, [OsStr::new("write-tree")])?;
                    tree = parse_object_id(&staged_tree.stdout)?;
                    successful.push(claim.token());
                }
                Err(error) => {
                    run_git(
                        Some(worktree),
                        None,
                        [
                            OsStr::new("read-tree"),
                            OsStr::new("--reset"),
                            OsStr::new("-u"),
                            OsStr::new(&tree),
                        ],
                    )?;
                    let Some(error_code) = error.request_error_code() else {
                        return Err(GitTransactionError::Apply {
                            request_id: claim.token().request_id(),
                            source: error,
                        });
                    };
                    failures.push(RequestFailure {
                        token: claim.token(),
                        error_code,
                    });
                }
            }
        }

        if successful.is_empty() {
            return Ok(BatchCommitOutcome::NoChanges { failures });
        }

        let stats = staged_stats(worktree, base)?;
        let message = commit_message(batch_id, claims, &successful, stats);
        commit(worktree, &self.identity, &message)?;
        let commit = resolve_in_worktree(worktree, "HEAD")?;
        before_publish(base, &commit)?;
        self.compare_and_swap_official(base, &commit)?;
        run_git(
            Some(&self.canonical_worktree),
            None,
            [
                OsStr::new("reset"),
                OsStr::new("--hard"),
                OsStr::new(&commit),
            ],
        )
        .map_err(|source| GitTransactionError::CanonicalWorktreeSync {
            commit: commit.clone(),
            source: Box::new(source),
        })?;

        let transaction_ref = format!("refs/heads/transactions/{batch_id}");
        self.remove_transaction(worktree, &transaction_ref, &commit)
            .map_err(|source| GitTransactionError::PostCommitCleanup {
                commit: commit.clone(),
                source: Box::new(source),
            })?;
        Ok(BatchCommitOutcome::Committed {
            commit,
            successful,
            failures,
        })
    }

    fn compare_and_swap_official(
        &self,
        expected: &str,
        commit: &str,
    ) -> Result<(), GitTransactionError> {
        let output = Command::new("git")
            .arg(format!("--git-dir={}", self.git_directory.display()))
            .args(["update-ref", &self.official_ref, commit, expected])
            .output()
            .map_err(GitTransactionError::Io)?;
        if output.status.success() {
            Ok(())
        } else {
            Err(GitTransactionError::OfficialBranchChanged {
                expected: expected.into(),
                actual: self.resolve_commit(&self.official_ref)?,
            })
        }
    }

    fn resolve_commit(&self, revision: &str) -> Result<String, GitTransactionError> {
        let expression = format!("{revision}^{{commit}}");
        let output = run_git(
            None,
            Some(&self.git_directory),
            [
                OsStr::new("rev-parse"),
                OsStr::new("--verify"),
                OsStr::new(&expression),
            ],
        )?;
        parse_object_id(&output.stdout)
    }

    fn remove_transaction(
        &self,
        worktree: &Path,
        transaction_ref: &str,
        expected_commit: &str,
    ) -> Result<(), GitTransactionError> {
        run_git(
            None,
            Some(&self.git_directory),
            [
                OsStr::new("worktree"),
                OsStr::new("remove"),
                OsStr::new("--force"),
                worktree.as_os_str(),
            ],
        )?;
        run_git(
            None,
            Some(&self.git_directory),
            [
                OsStr::new("update-ref"),
                OsStr::new("-d"),
                OsStr::new(transaction_ref),
                OsStr::new(expected_commit),
            ],
        )?;
        Ok(())
    }
}

fn ensure_real_directory(path: &Path) -> Result<(), GitTransactionError> {
    let metadata = fs::symlink_metadata(path).map_err(GitTransactionError::Io)?;
    if !metadata.file_type().is_dir() {
        return Err(GitTransactionError::InvalidDirectory(path.into()));
    }
    Ok(())
}

fn ensure_or_create_real_directory(path: &Path) -> Result<(), GitTransactionError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(()),
        Ok(_) => Err(GitTransactionError::InvalidDirectory(path.into())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir_all(path).map_err(GitTransactionError::Io)
        }
        Err(error) => Err(GitTransactionError::Io(error)),
    }
}

fn validate_nonoverlapping_paths(
    git_directory: &Path,
    canonical_worktree: &Path,
    work_root: &Path,
) -> Result<(), GitTransactionError> {
    let git_directory = fs::canonicalize(git_directory).map_err(GitTransactionError::Io)?;
    let canonical_worktree =
        fs::canonicalize(canonical_worktree).map_err(GitTransactionError::Io)?;
    let work_root = fs::canonicalize(work_root).map_err(GitTransactionError::Io)?;
    let paths = [&git_directory, &canonical_worktree, &work_root];
    for (index, left) in paths.iter().enumerate() {
        for right in paths.iter().skip(index + 1) {
            if left.starts_with(right) || right.starts_with(left) {
                return Err(GitTransactionError::OverlappingRepositoryPaths);
            }
        }
    }
    Ok(())
}

fn resolve_in_worktree(worktree: &Path, revision: &str) -> Result<String, GitTransactionError> {
    let output = run_git(
        Some(worktree),
        None,
        [
            OsStr::new("rev-parse"),
            OsStr::new("--verify"),
            OsStr::new(revision),
        ],
    )?;
    parse_object_id(&output.stdout)
}

fn commit(
    worktree: &Path,
    identity: &GitIdentity,
    message: &str,
) -> Result<(), GitTransactionError> {
    let name = format!("user.name={}", identity.name);
    let email = format!("user.email={}", identity.email);
    run_git_with_input(
        Some(worktree),
        None,
        [
            OsStr::new("-c"),
            OsStr::new(&name),
            OsStr::new("-c"),
            OsStr::new(&email),
            OsStr::new("commit"),
            OsStr::new("--no-gpg-sign"),
            OsStr::new("--file=-"),
        ],
        message.as_bytes(),
    )?;
    Ok(())
}

#[derive(Clone, Copy)]
struct FileStats {
    added: usize,
    modified: usize,
    deleted: usize,
}

fn staged_stats(worktree: &Path, base: &str) -> Result<FileStats, GitTransactionError> {
    let output = run_git(
        Some(worktree),
        None,
        [
            OsStr::new("diff"),
            OsStr::new("--cached"),
            OsStr::new("--name-status"),
            OsStr::new("--no-renames"),
            OsStr::new("-z"),
            OsStr::new(base),
        ],
    )?;
    let mut parts = output.stdout.split(|byte| *byte == 0);
    let mut stats = FileStats {
        added: 0,
        modified: 0,
        deleted: 0,
    };
    while let Some(status) = parts.next() {
        if status.is_empty() {
            break;
        }
        let Some(path) = parts.next() else {
            return Err(GitTransactionError::InvalidGitOutput);
        };
        if path.is_empty() {
            return Err(GitTransactionError::InvalidGitOutput);
        }
        match status.first() {
            Some(b'A') => stats.added += 1,
            Some(b'M') => stats.modified += 1,
            Some(b'D') => stats.deleted += 1,
            _ => return Err(GitTransactionError::InvalidGitOutput),
        }
    }
    Ok(stats)
}

fn commit_message(
    batch_id: BatchId,
    claims: &[ClaimedPackage],
    successful: &[ClaimToken],
    stats: FileStats,
) -> String {
    let mut message = format!("knowledge snapshot: {} changes\n\n", successful.len());
    for claim in claims {
        if successful.contains(&claim.token()) {
            let request = claim.package().request();
            message.push_str(&format!("- [{}] {}\n", request.request_id, request.title));
        }
    }
    message.push_str(&format!(
        "\nRequests: {}\nFiles-Added: {}\nFiles-Modified: {}\nFiles-Deleted: {}\nBatch-ID: {batch_id}\n",
        successful.len(),
        stats.added,
        stats.modified,
        stats.deleted
    ));
    for token in successful {
        message.push_str(&format!("Request-ID: {}\n", token.request_id()));
    }
    message
}

fn run_git<I, S>(
    working_directory: Option<&Path>,
    git_directory: Option<&Path>,
    arguments: I,
) -> Result<Output, GitTransactionError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    run_git_with_input(working_directory, git_directory, arguments, &[])
}

fn run_git_with_input<I, S>(
    working_directory: Option<&Path>,
    git_directory: Option<&Path>,
    arguments: I,
    input: &[u8],
) -> Result<Output, GitTransactionError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut command = Command::new("git");
    if let Some(working_directory) = working_directory {
        command.arg("-C").arg(working_directory);
    }
    if let Some(git_directory) = git_directory {
        command.arg(format!("--git-dir={}", git_directory.display()));
    }
    let arguments = arguments
        .into_iter()
        .map(|argument| argument.as_ref().to_os_string())
        .collect::<Vec<OsString>>();
    command
        .args(&arguments)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if input.is_empty() {
        command.stdin(Stdio::null());
    } else {
        command.stdin(Stdio::piped());
    }
    let mut child = command.spawn().map_err(GitTransactionError::Io)?;
    if !input.is_empty() {
        let mut stdin = child
            .stdin
            .take()
            .ok_or(GitTransactionError::InvalidGitOutput)?;
        stdin.write_all(input).map_err(GitTransactionError::Io)?;
    }
    let output = child.wait_with_output().map_err(GitTransactionError::Io)?;
    if output.status.success() {
        Ok(output)
    } else {
        Err(GitTransactionError::GitCommand {
            arguments,
            stderr: diagnostic(&output.stderr),
        })
    }
}

fn parse_text(output: &[u8]) -> Result<&str, GitTransactionError> {
    let value = std::str::from_utf8(output)
        .map_err(|_| GitTransactionError::InvalidGitOutput)?
        .trim();
    if value.is_empty() || value.chars().any(char::is_control) {
        return Err(GitTransactionError::InvalidGitOutput);
    }
    Ok(value)
}

fn parse_object_id(output: &[u8]) -> Result<String, GitTransactionError> {
    let value = parse_text(output)?;
    if !matches!(value.len(), 40 | 64)
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(GitTransactionError::InvalidGitOutput);
    }
    Ok(value.into())
}

fn diagnostic(stderr: &[u8]) -> String {
    let limit = stderr.len().min(MAXIMUM_DIAGNOSTIC_BYTES);
    String::from_utf8_lossy(&stderr[..limit]).into_owned()
}

/// A Git worktree transaction or publication failure.
#[derive(Debug)]
pub enum GitTransactionError {
    /// The configured author identity was empty or contained control characters.
    InvalidIdentity,
    /// The official branch name was empty or contained control characters.
    InvalidOfficialBranch,
    /// The configured repository was not bare.
    RepositoryNotBare,
    /// The canonical worktree belonged to a different Git repository.
    CanonicalWorktreeMismatch,
    /// The canonical worktree did not have the official branch checked out.
    CanonicalWorktreeBranchMismatch,
    /// Repository, canonical content, and disposable work paths overlapped.
    OverlappingRepositoryPaths,
    /// A required directory was a link or non-directory entry.
    InvalidDirectory(PathBuf),
    /// The batch contained no claims.
    EmptyBatch,
    /// The deterministic batch worktree path already existed.
    WorktreeAlreadyExists(PathBuf),
    /// A request failed for a non-isolatable reason.
    Apply {
        /// Request being applied.
        request_id: RequestId,
        /// Underlying apply failure.
        source: ApplyError,
    },
    /// The official branch changed after the transaction pinned its base.
    OfficialBranchChanged {
        /// Pinned base commit.
        expected: String,
        /// Commit found at publish time.
        actual: String,
    },
    /// The official ref advanced, but the canonical worktree could not be synchronized.
    CanonicalWorktreeSync {
        /// New official commit.
        commit: String,
        /// Git failure.
        source: Box<GitTransactionError>,
    },
    /// Publication completed but disposable transaction cleanup failed.
    PostCommitCleanup {
        /// New official commit.
        commit: String,
        /// Cleanup failure.
        source: Box<GitTransactionError>,
    },
    /// Git returned malformed machine-readable output.
    InvalidGitOutput,
    /// A Git subprocess failed.
    GitCommand {
        /// Fixed argument vector excluding the executable and repository path.
        arguments: Vec<OsString>,
        /// Bounded standard error.
        stderr: String,
    },
    /// A filesystem or subprocess I/O operation failed.
    Io(io::Error),
}

impl fmt::Display for GitTransactionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidIdentity => {
                formatter.write_str("Git identity values must be nonempty and contain no controls")
            }
            Self::InvalidOfficialBranch => formatter.write_str("official Git branch is invalid"),
            Self::RepositoryNotBare => formatter.write_str("Git repository must be bare"),
            Self::CanonicalWorktreeMismatch => {
                formatter.write_str("canonical worktree belongs to another Git repository")
            }
            Self::CanonicalWorktreeBranchMismatch => {
                formatter.write_str("canonical worktree must have the official branch checked out")
            }
            Self::OverlappingRepositoryPaths => formatter
                .write_str("Git repository, canonical worktree, and work root must not overlap"),
            Self::InvalidDirectory(path) => {
                write!(formatter, "`{}` must be a real directory", path.display())
            }
            Self::EmptyBatch => formatter.write_str("Git transaction batch must not be empty"),
            Self::WorktreeAlreadyExists(path) => {
                write!(
                    formatter,
                    "transaction worktree `{}` exists",
                    path.display()
                )
            }
            Self::Apply { request_id, source } => {
                write!(
                    formatter,
                    "request `{request_id}` could not be isolated: {source}"
                )
            }
            Self::OfficialBranchChanged { expected, actual } => write!(
                formatter,
                "official branch changed after pinning `{expected}`; found `{actual}`"
            ),
            Self::CanonicalWorktreeSync { commit, source } => write!(
                formatter,
                "official commit `{commit}` advanced but canonical worktree sync failed: {source}"
            ),
            Self::PostCommitCleanup { commit, source } => write!(
                formatter,
                "official commit `{commit}` advanced but transaction cleanup failed: {source}"
            ),
            Self::InvalidGitOutput => formatter.write_str("Git returned invalid output"),
            Self::GitCommand { arguments, stderr } => {
                write!(formatter, "Git command {arguments:?} failed: {stderr}")
            }
            Self::Io(error) => write!(formatter, "Git transaction I/O failed: {error}"),
        }
    }
}

impl std::error::Error for GitTransactionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Apply { source, .. } => Some(source),
            Self::CanonicalWorktreeSync { source, .. } | Self::PostCommitCleanup { source, .. } => {
                Some(source)
            }
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests;

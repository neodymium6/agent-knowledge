use std::collections::{HashMap, HashSet};
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Read, Write};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::Arc;

use agent_knowledge_core::{
    BatchId, ErrorCode, PathAttestation, PathAttestationError, RequestId, Revision,
};
use agent_knowledge_queue::{
    BatchReconciliation, ClaimToken, ClaimedPackage, PackagePolicy, WorkerQueueError, WorkerSession,
};
use agent_knowledge_release::{ReleaseError, ReleaseStore};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::apply::{AppliedMove, apply_claimed};
use crate::{ApplyError, ContentPolicy};

const MAXIMUM_DIAGNOSTIC_BYTES: usize = 8 * 1024;
const MAXIMUM_JOURNAL_BYTES: u64 = 1024 * 1024;
const MAXIMUM_BINDING_BYTES: usize = 64 * 1024;
const PREVIOUS_JOURNAL_SCHEMA_VERSION: u16 = 2;
const JOURNAL_SCHEMA_VERSION: u16 = 3;

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

/// Callbacks that build and durably prepare derived output around one commit.
pub struct BatchPublication<F, H> {
    trial_build: F,
    before_publish: H,
}

/// Bounded signal returned when a derived-publication callback fails.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublicationError;

impl PublicationError {
    /// Creates a callback failure after the caller retained its detailed error.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for PublicationError {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for PublicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("derived publication callback failed")
    }
}

impl std::error::Error for PublicationError {}

impl<F, H> BatchPublication<F, H> {
    /// Groups the trial build and pre-publication preparation callbacks.
    #[must_use]
    pub const fn new(trial_build: F, before_publish: H) -> Self {
        Self {
            trial_build,
            before_publish,
        }
    }
}

/// Central bare repository and its canonical linked worktree.
#[derive(Clone, Debug)]
pub struct GitRepository {
    git_directory: PathBuf,
    configured_git_directory: PathBuf,
    git_root_handle: Arc<File>,
    canonical_worktree: PathBuf,
    configured_canonical_worktree: PathBuf,
    canonical_root_handle: Arc<File>,
    configured_work_root: PathBuf,
    work_root: PathBuf,
    work_root_handle: Arc<File>,
    configured_journal_root: PathBuf,
    journal_root: PathBuf,
    journal_root_handle: Arc<File>,
    configured_worktree_root: PathBuf,
    worktree_root: PathBuf,
    worktree_root_handle: Arc<File>,
    binding_file: PathBuf,
    work_root_binding_file: PathBuf,
    official_ref: String,
    identity: GitIdentity,
}

/// Durable repository work that must be resumed before a new batch starts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RepositoryTransaction {
    /// Claims were recorded, but no terminal repository outcome exists yet.
    Preparing {
        /// Batch that owns the transaction.
        batch_id: BatchId,
        /// Requests rejected while the batch was claimed.
        claim_failures: usize,
    },
    /// A terminal outcome exists and publication or reconciliation must resume.
    Recoverable {
        /// Batch that owns the transaction.
        batch_id: BatchId,
        /// Requests rejected while the batch was claimed.
        claim_failures: usize,
    },
}

/// Ordered claims and queue-stage outcomes entering one repository transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClaimedBatch<'a> {
    claims: &'a [ClaimedPackage],
    claim_failures: usize,
}

impl<'a> ClaimedBatch<'a> {
    /// Creates one batch from ordered claims and prior validation failures.
    #[must_use]
    pub const fn new(claims: &'a [ClaimedPackage], claim_failures: usize) -> Self {
        Self {
            claims,
            claim_failures,
        }
    }

    /// Returns the ordered, durably owned requests.
    #[must_use]
    pub const fn claims(self) -> &'a [ClaimedPackage] {
        self.claims
    }

    /// Returns requests rejected before repository processing began.
    #[must_use]
    pub const fn claim_failures(self) -> usize {
        self.claim_failures
    }
}

impl RepositoryTransaction {
    /// Returns the batch that owns this durable transaction.
    #[must_use]
    pub const fn batch_id(self) -> BatchId {
        match self {
            Self::Preparing { batch_id, .. } | Self::Recoverable { batch_id, .. } => batch_id,
        }
    }

    /// Returns requests rejected before repository processing began.
    #[must_use]
    pub const fn claim_failures(self) -> usize {
        match self {
            Self::Preparing { claim_failures, .. } | Self::Recoverable { claim_failures, .. } => {
                claim_failures
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct TransactionJournal {
    schema_version: u16,
    batch_id: BatchId,
    queue_identity: Revision,
    base_commit: String,
    claims: Vec<JournalClaim>,
    claim_failures: u64,
    state: JournalState,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum StoredTransactionJournal {
    Current(TransactionJournal),
    Previous(PreviousTransactionJournal),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreviousTransactionJournal {
    schema_version: u16,
    batch_id: BatchId,
    queue_identity: Revision,
    base_commit: String,
    claims: Vec<JournalClaim>,
    state: JournalState,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct JournalClaim {
    request_id: RequestId,
    attempt: u32,
    acceptance_sequence: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
enum JournalState {
    Preparing,
    NoChanges {
        failures: Vec<JournalFailure>,
    },
    Committed {
        commit: String,
        successful: Vec<RequestId>,
        failures: Vec<JournalFailure>,
        publication_started: bool,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct JournalFailure {
    request_id: RequestId,
    error_code: ErrorCode,
}

impl GitRepository {
    /// Attests the repository, canonical content, and disposable work roots.
    ///
    /// # Errors
    ///
    /// Returns an error when a configured root no longer names its pinned
    /// object or its ancestry cannot be inspected.
    pub fn storage_attestations(&self) -> Result<[PathAttestation; 3], PathAttestationError> {
        Ok([
            PathAttestation::capture(&self.configured_git_directory, &self.git_root_handle)?,
            PathAttestation::capture(
                &self.configured_canonical_worktree,
                &self.canonical_root_handle,
            )?,
            PathAttestation::capture(&self.configured_work_root, &self.work_root_handle)?,
        ])
    }

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
        ensure_supported_git()?;
        ensure_real_directory(git_directory)?;
        let configured_git_directory =
            fs::canonicalize(git_directory).map_err(GitTransactionError::Io)?;
        if official_branch.is_empty() || official_branch.chars().any(char::is_control) {
            return Err(GitTransactionError::InvalidOfficialBranch);
        }
        let official_ref = format!("refs/heads/{official_branch}");
        run_git(
            None,
            Some(&configured_git_directory),
            [OsStr::new("check-ref-format"), OsStr::new(&official_ref)],
        )?;
        validate_local_git_config(&configured_git_directory)?;
        ensure_real_directory(canonical_worktree)?;
        let configured_canonical_worktree =
            fs::canonicalize(canonical_worktree).map_err(GitTransactionError::Io)?;
        ensure_or_create_real_directory(work_root)?;
        let configured_work_root = fs::canonicalize(work_root).map_err(GitTransactionError::Io)?;
        validate_repository_layout(
            &configured_git_directory,
            &configured_canonical_worktree,
            &official_ref,
        )?;
        validate_nonoverlapping_paths(
            &configured_git_directory,
            &configured_canonical_worktree,
            &configured_work_root,
        )?;
        let (git_root_handle, git_directory) = open_stable_directory(&configured_git_directory)?;
        let (canonical_root_handle, canonical_worktree) =
            open_stable_directory(&configured_canonical_worktree)?;
        let (work_root_handle, work_root) = open_stable_directory(&configured_work_root)?;
        let configured_journal_root = configured_work_root.join("transactions");
        let configured_worktree_root = configured_work_root.join("worktrees");
        ensure_or_create_real_directory(&work_root.join("transactions"))?;
        ensure_or_create_real_directory(&work_root.join("worktrees"))?;
        let (journal_root_handle, journal_root) = open_stable_directory(&configured_journal_root)?;
        let (worktree_root_handle, worktree_root) =
            open_stable_directory(&configured_worktree_root)?;
        let repository_state = git_directory.join("agent-knowledge");
        ensure_or_create_real_directory(&repository_state)?;
        let writer = lock_root_paths(
            &git_directory,
            &configured_git_directory,
            &work_root,
            &configured_work_root,
        )?;
        let binding_directories = [
            (configured_git_directory.as_path(), git_root_handle.as_ref()),
            (
                configured_canonical_worktree.as_path(),
                canonical_root_handle.as_ref(),
            ),
            (configured_work_root.as_path(), work_root_handle.as_ref()),
            (
                configured_journal_root.as_path(),
                journal_root_handle.as_ref(),
            ),
            (
                configured_worktree_root.as_path(),
                worktree_root_handle.as_ref(),
            ),
        ];
        let expected_binding = repository_binding(&binding_directories, &official_ref)?;
        let binding_file = repository_state.join("binding-v2");
        ensure_binding(&binding_file, &expected_binding)?;
        let work_root_binding_file = work_root.join(".agent-knowledge-repository-binding-v2");
        ensure_binding(&work_root_binding_file, &expected_binding)?;
        drop(writer);
        sync_directory(&repository_state).map_err(GitTransactionError::Io)?;
        sync_directory(&work_root).map_err(GitTransactionError::Io)?;

        Ok(Self {
            git_directory,
            configured_git_directory,
            git_root_handle,
            canonical_worktree,
            configured_canonical_worktree,
            canonical_root_handle,
            configured_work_root,
            work_root,
            work_root_handle,
            configured_journal_root,
            journal_root,
            journal_root_handle,
            configured_worktree_root,
            worktree_root,
            worktree_root_handle,
            binding_file,
            work_root_binding_file,
            official_ref,
            identity,
        })
    }

    /// Discovers the single durable transaction that startup must resume.
    ///
    /// # Errors
    ///
    /// Returns an error when queue recovery is incomplete, more than one
    /// transaction exists, or the journal does not match this queue and
    /// repository.
    pub fn unfinished_transaction(
        &self,
        worker: &WorkerSession,
    ) -> Result<Option<RepositoryTransaction>, GitTransactionError> {
        self.discover_unfinished_transaction(worker, true)
    }

    /// Discovers durable transaction metadata while queue recovery is in progress.
    ///
    /// This read-only operation validates the repository, journal, and queue
    /// binding but does not authorize transaction replay. Call
    /// [`Self::unfinished_transaction`] after queue recovery before mutating
    /// repository state.
    ///
    /// # Errors
    ///
    /// Returns an error when storage or journal validation fails.
    pub fn unfinished_transaction_summary(
        &self,
        worker: &WorkerSession,
    ) -> Result<Option<RepositoryTransaction>, GitTransactionError> {
        self.discover_unfinished_transaction(worker, false)
    }

    fn discover_unfinished_transaction(
        &self,
        worker: &WorkerSession,
        require_transaction_ready: bool,
    ) -> Result<Option<RepositoryTransaction>, GitTransactionError> {
        let _writer = self.lock_writer()?;
        if require_transaction_ready {
            worker
                .ensure_transaction_ready()
                .map_err(GitTransactionError::Queue)?;
        } else {
            worker
                .queue_identity()
                .map_err(GitTransactionError::Queue)?;
        }
        self.validate_live_storage()?;

        let mut transaction = None;
        for entry in fs::read_dir(&self.journal_root).map_err(GitTransactionError::Io)? {
            let entry = entry.map_err(GitTransactionError::Io)?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| GitTransactionError::InvalidJournal)?;
            let Some(batch_text) = name.strip_suffix(".json") else {
                continue;
            };
            let batch_id = batch_text
                .parse::<BatchId>()
                .map_err(|_| GitTransactionError::InvalidJournal)?;
            if batch_id.to_string() != batch_text || transaction.is_some() {
                return Err(GitTransactionError::UnfinishedTransaction);
            }
            let journal =
                read_journal(&entry.path())?.ok_or(GitTransactionError::JournalMissing)?;
            validate_journal_structure(&journal, batch_id)?;
            validate_worker_identity(&journal, worker)?;
            let claim_failures = usize::try_from(journal.claim_failures)
                .map_err(|_| GitTransactionError::JournalMismatch)?;
            transaction = Some(match journal.state {
                JournalState::Preparing => RepositoryTransaction::Preparing {
                    batch_id,
                    claim_failures,
                },
                JournalState::NoChanges { .. } | JournalState::Committed { .. } => {
                    RepositoryTransaction::Recoverable {
                        batch_id,
                        claim_failures,
                    }
                }
            });
        }
        Ok(transaction)
    }

    /// Applies a bounded ordered claim batch, requires a successful trial build
    /// of the exact prepared worktree, creates one commit, and advances the
    /// official branch with an expected-old-commit check.
    ///
    /// Request-specific deterministic failures are rolled back to the last
    /// successful index tree and returned separately. Infrastructure or
    /// content-store or trial-build failures abort the batch without advancing
    /// the official branch.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty batch, Git or filesystem failure,
    /// non-request content failure, failed trial build, or concurrent
    /// official-branch change.
    pub fn apply_batch<F>(
        &self,
        worker: &mut WorkerSession,
        batch_id: BatchId,
        claims: &[ClaimedPackage],
        policy: ContentPolicy,
        package_policy: &PackagePolicy,
        trial_build: F,
    ) -> Result<BatchCommitOutcome, GitTransactionError>
    where
        F: FnOnce(&Path) -> Result<(), GitTransactionError>,
    {
        self.apply_batch_with_hook(
            worker,
            batch_id,
            ClaimedBatch::new(claims, 0),
            policy,
            package_policy,
            TransactionHooks {
                trial_build,
                before_publish: continue_publication,
            },
        )
    }

    /// Applies a batch and invokes a durable publication callback after the
    /// commit journal exists but before the official branch can advance.
    ///
    /// The callback receives the exact prepared content worktree and commit.
    /// It is intended for preparing derived output that recovery must be able
    /// to resume before repository publication.
    ///
    /// # Errors
    ///
    /// Returns the same failures as [`Self::apply_batch`] or a callback error.
    pub fn apply_batch_with_publication<F, H>(
        &self,
        worker: &mut WorkerSession,
        batch_id: BatchId,
        batch: ClaimedBatch<'_>,
        policy: ContentPolicy,
        package_policy: &PackagePolicy,
        publication: BatchPublication<F, H>,
    ) -> Result<BatchCommitOutcome, GitTransactionError>
    where
        F: FnOnce(&Path) -> Result<(), PublicationError>,
        H: FnOnce(&Path, &str) -> Result<(), PublicationError>,
    {
        let BatchPublication {
            trial_build,
            before_publish,
        } = publication;
        self.apply_batch_with_hook(
            worker,
            batch_id,
            batch,
            policy,
            package_policy,
            TransactionHooks {
                trial_build: |content: &Path| {
                    trial_build(content).map_err(|_| GitTransactionError::TrialBuildFailed)
                },
                before_publish: |_: &str, commit: &str| {
                    before_publish(&self.worktree_path(batch_id), commit)
                        .map_err(|_| GitTransactionError::TrialBuildFailed)
                },
            },
        )
    }

    fn apply_batch_with_hook<F, H>(
        &self,
        worker: &mut WorkerSession,
        batch_id: BatchId,
        batch: ClaimedBatch<'_>,
        policy: ContentPolicy,
        package_policy: &PackagePolicy,
        hooks: TransactionHooks<F, H>,
    ) -> Result<BatchCommitOutcome, GitTransactionError>
    where
        F: FnOnce(&Path) -> Result<(), GitTransactionError>,
        H: FnOnce(&str, &str) -> Result<(), GitTransactionError>,
    {
        let _writer = self.lock_writer()?;
        let TransactionHooks {
            trial_build,
            before_publish,
        } = hooks;
        let claims = batch.claims();
        if claims.is_empty() {
            return Err(GitTransactionError::EmptyBatch);
        }
        let journal_claim_failures = u64::try_from(batch.claim_failures())
            .map_err(|_| GitTransactionError::JournalMismatch)?;
        self.ensure_no_other_journal(batch_id)?;
        let journal_path = self.journal_path(batch_id);
        if let Some(journal) = read_journal(&journal_path)? {
            validate_journal_structure(&journal, batch_id)?;
            validate_worker_identity(&journal, worker)?;
            match journal.state {
                JournalState::Preparing => {
                    let actual = self.resolve_commit(&self.official_ref)?;
                    if actual != journal.base_commit {
                        return Err(GitTransactionError::OfficialBranchChanged {
                            expected: journal.base_commit,
                            actual,
                        });
                    }
                    validate_batch_claims(worker, batch_id, claims)?;
                    validate_journal_claims(&journal, claims)?;
                    if journal.claim_failures != journal_claim_failures {
                        return Err(GitTransactionError::JournalMismatch);
                    }
                    self.remove_worktree(&self.worktree_path(batch_id))?;
                    self.remove_preparing_ref(batch_id, &journal.base_commit)?;
                    remove_journal(&journal_path)?;
                }
                JournalState::NoChanges { .. } | JournalState::Committed { .. } => {
                    return Err(GitTransactionError::TransactionRequiresRecovery { batch_id });
                }
            }
        }

        validate_batch_claims(worker, batch_id, claims)?;
        self.ensure_canonical_clean()?;
        let base = self.resolve_commit(&self.official_ref)?;
        let worktree = self.worktree_path(batch_id);
        match fs::symlink_metadata(&worktree) {
            Ok(_) => {
                return Err(GitTransactionError::WorktreeAlreadyExists(worktree));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(GitTransactionError::Io(error)),
        }
        let preparing = TransactionJournal {
            schema_version: JOURNAL_SCHEMA_VERSION,
            batch_id,
            queue_identity: worker
                .queue_identity()
                .map_err(GitTransactionError::Queue)?,
            base_commit: base.clone(),
            claims: journal_claims(claims),
            claim_failures: journal_claim_failures,
            state: JournalState::Preparing,
        };
        write_journal(&journal_path, &preparing)?;

        run_git(
            None,
            Some(&self.git_directory),
            [
                OsStr::new("worktree"),
                OsStr::new("add"),
                OsStr::new("--detach"),
                worktree.as_os_str(),
                OsStr::new(&base),
            ],
        )?;
        sync_directory(&self.worktree_root).map_err(GitTransactionError::Io)?;

        let prepared = self.apply_in_worktree(worker, claims, policy, package_policy, &worktree)?;
        if prepared.successful.is_empty() {
            let no_changes = TransactionJournal {
                state: JournalState::NoChanges {
                    failures: prepared
                        .failures
                        .iter()
                        .map(|failure| JournalFailure {
                            request_id: failure.token.request_id(),
                            error_code: failure.error_code,
                        })
                        .collect(),
                },
                ..preparing
            };
            write_journal(&journal_path, &no_changes)?;
            self.remove_worktree(&worktree)?;
            let actual = self.resolve_commit(&self.official_ref)?;
            if actual != base {
                return Err(GitTransactionError::OfficialBranchChanged {
                    expected: base,
                    actual,
                });
            }
            self.validate_live_storage()?;
            return Ok(BatchCommitOutcome::NoChanges {
                failures: prepared.failures,
            });
        }

        let stats = staged_stats(&worktree, &base, &prepared.moves)?;
        trial_build(&worktree)?;
        self.ensure_prepared_worktree_unchanged(&worktree, &prepared.tree)?;
        let message = commit_message(batch_id, claims, &prepared.successful, stats);
        let commit = commit_tree(
            &self.git_directory,
            &self.identity,
            &prepared.tree,
            &base,
            &message,
        )?;
        self.create_transaction_ref(batch_id, &commit)?;
        let mut committed = TransactionJournal {
            state: JournalState::Committed {
                commit: commit.clone(),
                successful: prepared
                    .successful
                    .iter()
                    .map(|token| token.request_id())
                    .collect(),
                failures: prepared
                    .failures
                    .iter()
                    .map(|failure| JournalFailure {
                        request_id: failure.token.request_id(),
                        error_code: failure.error_code,
                    })
                    .collect(),
                publication_started: false,
            },
            ..preparing
        };
        write_journal(&journal_path, &committed)?;
        self.validate_committed_journal(&committed)?;
        before_publish(&base, &commit)?;
        let JournalState::Committed {
            publication_started,
            ..
        } = &mut committed.state
        else {
            return Err(GitTransactionError::JournalState);
        };
        *publication_started = true;
        write_journal(&journal_path, &committed)?;
        self.publish_committed(batch_id, &base, &commit, true)?;
        Ok(BatchCommitOutcome::Committed {
            commit,
            successful: prepared.successful,
            failures: prepared.failures,
        })
    }

    fn apply_in_worktree(
        &self,
        worker: &mut WorkerSession,
        claims: &[ClaimedPackage],
        policy: ContentPolicy,
        package_policy: &PackagePolicy,
        worktree: &Path,
    ) -> Result<PreparedBatch, GitTransactionError> {
        let mut tree = resolve_in_worktree(worktree, "HEAD^{tree}")?;
        let mut successful = Vec::new();
        let mut failures = Vec::new();
        let mut moves = Vec::new();

        for claim in claims {
            worker
                .validate_claimed(claim)
                .map_err(GitTransactionError::Queue)?;
            match apply_claimed(worktree, claim, policy, package_policy) {
                Ok(outcome) => {
                    run_git(
                        Some(worktree),
                        None,
                        [
                            OsStr::new("add"),
                            OsStr::new("--all"),
                            OsStr::new("--force"),
                        ],
                    )?;
                    let staged_tree = run_git(Some(worktree), None, [OsStr::new("write-tree")])?;
                    tree = parse_object_id(&staged_tree.stdout)?;
                    successful.push(claim.token());
                    moves.extend_from_slice(outcome.moves());
                }
                Err(error) => {
                    reset_worktree(worktree, &tree)?;
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

        Ok(PreparedBatch {
            tree,
            successful,
            failures,
            moves,
        })
    }

    /// Discards a still-unpublished preparing transaction so its claims can be
    /// requeued and replayed in smaller groups after a deterministic build
    /// failure.
    ///
    /// # Errors
    ///
    /// Returns an error unless the journal, live claims, queue instance, and
    /// unchanged official branch all match exactly.
    pub fn abort_preparing_batch(
        &self,
        worker: &mut WorkerSession,
        batch_id: BatchId,
        claims: &[ClaimedPackage],
    ) -> Result<(), GitTransactionError> {
        self.abort_preparing_batch_with_hook(worker, batch_id, claims, || Ok(()))
    }

    fn abort_preparing_batch_with_hook<F>(
        &self,
        worker: &mut WorkerSession,
        batch_id: BatchId,
        claims: &[ClaimedPackage],
        before_final_validation: F,
    ) -> Result<(), GitTransactionError>
    where
        F: FnOnce() -> Result<(), GitTransactionError>,
    {
        let _writer = self.lock_writer()?;
        self.ensure_no_other_journal(batch_id)?;
        let journal_path = self.journal_path(batch_id);
        let journal = read_journal(&journal_path)?.ok_or(GitTransactionError::JournalMissing)?;
        validate_journal_structure(&journal, batch_id)?;
        validate_worker_identity(&journal, worker)?;
        if !matches!(journal.state, JournalState::Preparing) {
            return Err(GitTransactionError::JournalState);
        }
        validate_batch_claims(worker, batch_id, claims)?;
        validate_journal_claims(&journal, claims)?;
        let actual = self.resolve_commit(&self.official_ref)?;
        if actual != journal.base_commit {
            return Err(GitTransactionError::OfficialBranchChanged {
                expected: journal.base_commit,
                actual,
            });
        }
        self.remove_worktree(&self.worktree_path(batch_id))?;
        self.remove_preparing_ref(batch_id, &journal.base_commit)?;
        remove_journal(&journal_path)?;
        before_final_validation()?;
        self.validate_live_storage()
    }

    /// Removes a terminal transaction journal after exact queue reconciliation
    /// and, for a committed batch, activation of its immutable release.
    ///
    /// # Errors
    ///
    /// Returns an error if the journal, queue proof, outcome, or active release
    /// do not describe exactly the same terminal batch.
    pub fn finalize_batch(
        &self,
        worker: &WorkerSession,
        batch_id: BatchId,
        outcome: &BatchCommitOutcome,
        reconciliation: &BatchReconciliation,
        release_store: Option<&ReleaseStore>,
    ) -> Result<(), GitTransactionError> {
        let _writer = self.lock_writer()?;
        worker
            .ensure_transaction_ready()
            .map_err(GitTransactionError::Queue)?;
        let journal_path = self.journal_path(batch_id);
        let journal = read_journal(&journal_path)?.ok_or(GitTransactionError::JournalMissing)?;
        validate_journal_structure(&journal, batch_id)?;
        validate_worker_identity(&journal, worker)?;
        if matches!(&journal.state, JournalState::Committed { .. }) {
            self.validate_committed_journal(&journal)?;
        }
        let journal_outcome = outcome_from_journal(&journal)?;
        if journal_outcome != *outcome {
            return Err(GitTransactionError::JournalMismatch);
        }
        let (commit, successful, failures) = outcome_parts(outcome);
        let queue_identity = worker
            .queue_identity()
            .map_err(GitTransactionError::Queue)?;
        if !reconciliation.validates(queue_identity, batch_id, successful, &failures) {
            return Err(GitTransactionError::JournalMismatch);
        }
        match (commit, release_store) {
            (Some(commit), Some(releases))
                if releases
                    .active_release()
                    .map_err(|error| GitTransactionError::Release(Box::new(error)))?
                    .is_some_and(|active| active.commit() == commit)
                    && self.resolve_commit(&self.official_ref)? == commit => {}
            (None, None) if self.resolve_commit(&self.official_ref)? == journal.base_commit => {}
            _ => return Err(GitTransactionError::PublicationIncomplete),
        }
        if path_exists(&self.worktree_path(batch_id))?
            || self
                .resolve_optional_commit(&transaction_ref(batch_id))?
                .is_some()
        {
            return Err(GitTransactionError::JournalState);
        }
        remove_journal(&journal_path)
    }

    #[cfg(test)]
    fn finalize_batch_without_publication_proofs(
        &self,
        worker: &WorkerSession,
        batch_id: BatchId,
        commit: Option<&str>,
    ) -> Result<(), GitTransactionError> {
        let _writer = self.lock_writer()?;
        worker
            .ensure_transaction_ready()
            .map_err(GitTransactionError::Queue)?;
        let journal_path = self.journal_path(batch_id);
        let journal = read_journal(&journal_path)?.ok_or(GitTransactionError::JournalMissing)?;
        validate_journal_structure(&journal, batch_id)?;
        validate_worker_identity(&journal, worker)?;
        if matches!(&journal.state, JournalState::Committed { .. }) {
            self.validate_committed_journal(&journal)?;
        }
        match journal.state {
            JournalState::Committed {
                commit: journal_commit,
                ..
            } if commit == Some(journal_commit.as_str())
                && self.resolve_commit(&self.official_ref)? == journal_commit => {}
            JournalState::NoChanges { .. }
                if commit.is_none()
                    && self.resolve_commit(&self.official_ref)? == journal.base_commit => {}
            _ => return Err(GitTransactionError::JournalMismatch),
        }
        if path_exists(&self.worktree_path(batch_id))?
            || self
                .resolve_optional_commit(&transaction_ref(batch_id))?
                .is_some()
        {
            return Err(GitTransactionError::JournalState);
        }
        remove_journal(&journal_path)
    }

    #[cfg(test)]
    fn recover_batch(
        &self,
        worker: &WorkerSession,
        batch_id: BatchId,
    ) -> Result<BatchCommitOutcome, GitTransactionError> {
        self.recover_batch_with_publication(worker, batch_id, |_, _| Ok(()))
    }

    /// Recovers a terminal transaction while ensuring its durable derived
    /// publication is prepared before the official branch can advance.
    ///
    /// The callback runs only when publication had not durably started. It
    /// receives the retained prepared worktree and exact journaled commit.
    ///
    /// # Errors
    ///
    /// Returns an error when the journal is absent or malformed, publication
    /// cannot be resumed, disposable cleanup fails, or the callback fails.
    pub fn recover_batch_with_publication<F>(
        &self,
        worker: &WorkerSession,
        batch_id: BatchId,
        before_publish: F,
    ) -> Result<BatchCommitOutcome, GitTransactionError>
    where
        F: FnOnce(&Path, &str) -> Result<(), PublicationError>,
    {
        let _writer = self.lock_writer()?;
        worker
            .ensure_transaction_ready()
            .map_err(GitTransactionError::Queue)?;
        self.ensure_no_other_journal(batch_id)?;
        let journal_path = self.journal_path(batch_id);
        let mut journal =
            read_journal(&journal_path)?.ok_or(GitTransactionError::JournalMissing)?;
        validate_journal_structure(&journal, batch_id)?;
        validate_worker_identity(&journal, worker)?;
        let committed = match &journal.state {
            JournalState::Committed {
                commit,
                publication_started,
                ..
            } => Some((commit.clone(), *publication_started)),
            _ => None,
        };
        if let Some((commit, publication_started)) = committed {
            self.validate_committed_journal(&journal)?;
            if !publication_started {
                let actual = self.resolve_commit(&self.official_ref)?;
                if actual != journal.base_commit {
                    return Err(GitTransactionError::OfficialBranchChanged {
                        expected: journal.base_commit.clone(),
                        actual,
                    });
                }
                before_publish(&self.worktree_path(batch_id), &commit)
                    .map_err(|_| GitTransactionError::TrialBuildFailed)?;
                let JournalState::Committed {
                    publication_started,
                    ..
                } = &mut journal.state
                else {
                    return Err(GitTransactionError::JournalState);
                };
                *publication_started = true;
                write_journal(&journal_path, &journal)?;
            }
            self.publish_committed(batch_id, &journal.base_commit, &commit, true)?;
        } else {
            match &journal.state {
                JournalState::NoChanges { .. } => {
                    let actual = self.resolve_commit(&self.official_ref)?;
                    if actual != journal.base_commit {
                        return Err(GitTransactionError::OfficialBranchChanged {
                            expected: journal.base_commit.clone(),
                            actual,
                        });
                    }
                    self.remove_worktree(&self.worktree_path(batch_id))?;
                    self.remove_preparing_ref(batch_id, &journal.base_commit)?;
                }
                JournalState::Preparing => return Err(GitTransactionError::JournalState),
                JournalState::Committed { .. } => {
                    return Err(GitTransactionError::JournalState);
                }
            }
        }
        self.validate_live_storage()?;
        outcome_from_journal(&journal)
    }

    fn publish_committed(
        &self,
        batch_id: BatchId,
        base: &str,
        commit: &str,
        publication_started: bool,
    ) -> Result<(), GitTransactionError> {
        self.validate_live_storage()?;
        let canonical_lock =
            File::open(&self.canonical_worktree).map_err(GitTransactionError::Io)?;
        canonical_lock.lock().map_err(GitTransactionError::Io)?;
        let actual = self.resolve_commit(&self.official_ref)?;
        if actual == base {
            self.ensure_canonical_clean()?;
            self.ensure_transaction_ref(batch_id, commit)?;
            self.validate_live_storage()?;
            self.compare_and_swap_official(base, commit)?;
        } else if actual != commit {
            return Err(GitTransactionError::OfficialBranchChanged {
                expected: base.into(),
                actual,
            });
        } else if publication_started {
            self.ensure_canonical_repairable(base)?;
        } else {
            return Err(GitTransactionError::JournalMismatch);
        }
        self.sync_canonical(commit)?;
        self.remove_worktree(&self.worktree_path(batch_id))
            .and_then(|()| self.remove_transaction_ref(batch_id, Some(commit)))
            .map_err(|source| GitTransactionError::PostCommitCleanup {
                commit: commit.into(),
                source: Box::new(source),
            })?;
        self.validate_live_storage()
    }

    fn lock_writer(&self) -> Result<RepositoryWriter, GitTransactionError> {
        let writer = lock_root_paths(
            &self.git_directory,
            &self.configured_git_directory,
            &self.work_root,
            &self.configured_work_root,
        )?;
        self.validate_live_storage()?;
        Ok(writer)
    }

    fn validate_live_storage(&self) -> Result<(), GitTransactionError> {
        validate_pinned_directory(&self.configured_git_directory, &self.git_root_handle)?;
        validate_pinned_directory(
            &self.configured_canonical_worktree,
            &self.canonical_root_handle,
        )?;
        validate_pinned_directory(&self.configured_work_root, &self.work_root_handle)?;
        validate_pinned_directory(&self.configured_journal_root, &self.journal_root_handle)?;
        validate_pinned_directory(&self.configured_worktree_root, &self.worktree_root_handle)?;
        validate_nonoverlapping_paths(
            &self.configured_git_directory,
            &self.configured_canonical_worktree,
            &self.configured_work_root,
        )?;
        validate_local_git_config(&self.git_directory)?;
        validate_repository_layout(
            &self.git_directory,
            &self.canonical_worktree,
            &self.official_ref,
        )?;
        let binding_directories = [
            (
                self.configured_git_directory.as_path(),
                self.git_root_handle.as_ref(),
            ),
            (
                self.configured_canonical_worktree.as_path(),
                self.canonical_root_handle.as_ref(),
            ),
            (
                self.configured_work_root.as_path(),
                self.work_root_handle.as_ref(),
            ),
            (
                self.configured_journal_root.as_path(),
                self.journal_root_handle.as_ref(),
            ),
            (
                self.configured_worktree_root.as_path(),
                self.worktree_root_handle.as_ref(),
            ),
        ];
        let expected_binding = repository_binding(&binding_directories, &self.official_ref)?;
        validate_binding(&self.binding_file, &expected_binding)?;
        validate_binding(&self.work_root_binding_file, &expected_binding)
    }

    fn validate_committed_journal(
        &self,
        journal: &TransactionJournal,
    ) -> Result<(), GitTransactionError> {
        let JournalState::Committed {
            commit, successful, ..
        } = &journal.state
        else {
            return Err(GitTransactionError::JournalState);
        };
        if self.resolve_commit(commit)? != *commit {
            return Err(GitTransactionError::JournalMismatch);
        }
        let parents = run_git(
            None,
            Some(&self.git_directory),
            [
                OsStr::new("rev-list"),
                OsStr::new("--parents"),
                OsStr::new("-n"),
                OsStr::new("1"),
                OsStr::new(commit),
            ],
        )?;
        let parents = parse_text(&parents.stdout)?
            .split_ascii_whitespace()
            .collect::<Vec<_>>();
        if parents.as_slice() != [commit.as_str(), journal.base_commit.as_str()] {
            return Err(GitTransactionError::JournalMismatch);
        }
        let message = run_git(
            None,
            Some(&self.git_directory),
            [
                OsStr::new("show"),
                OsStr::new("-s"),
                OsStr::new("--format=%B"),
                OsStr::new(commit),
            ],
        )?;
        let message = std::str::from_utf8(&message.stdout)
            .map_err(|_| GitTransactionError::InvalidGitOutput)?;
        let batch_trailers = message
            .lines()
            .filter(|line| line.starts_with("Batch-ID:"))
            .collect::<Vec<_>>();
        let request_trailers = message
            .lines()
            .filter(|line| line.starts_with("Knowledge-Request:"))
            .collect::<Vec<_>>();
        let expected_requests = successful
            .iter()
            .map(|request_id| format!("Knowledge-Request: {request_id}"))
            .collect::<Vec<_>>();
        if batch_trailers.len() != 1
            || batch_trailers[0] != format!("Batch-ID: {}", journal.batch_id)
            || request_trailers != expected_requests
        {
            return Err(GitTransactionError::JournalMismatch);
        }
        Ok(())
    }

    fn ensure_canonical_clean(&self) -> Result<(), GitTransactionError> {
        ensure_canonical_worktree_clean(&self.canonical_worktree)
    }

    fn ensure_prepared_worktree_unchanged(
        &self,
        worktree: &Path,
        expected_tree: &str,
    ) -> Result<(), GitTransactionError> {
        let unstaged = run_git(
            Some(worktree),
            None,
            [
                OsStr::new("diff"),
                OsStr::new("--name-only"),
                OsStr::new("-z"),
            ],
        )?;
        let untracked = untracked_files(worktree)?;
        let actual_tree = run_git(Some(worktree), None, [OsStr::new("write-tree")])?;
        if !unstaged.stdout.is_empty()
            || !untracked.is_empty()
            || parse_object_id(&actual_tree.stdout)? != expected_tree
        {
            return Err(GitTransactionError::TrialBuildMutatedWorktree);
        }
        Ok(())
    }

    fn ensure_canonical_repairable(&self, base: &str) -> Result<(), GitTransactionError> {
        for path in untracked_files(&self.canonical_worktree)? {
            let base_object = resolve_tree_path(&self.git_directory, base, &path)?;
            let Some(base_object) = base_object else {
                return Err(GitTransactionError::CanonicalWorktreeDirty);
            };
            let current_object = hash_worktree_file(&self.canonical_worktree, &path)?;
            if current_object != base_object {
                return Err(GitTransactionError::CanonicalWorktreeDirty);
            }
        }
        Ok(())
    }

    fn sync_canonical(&self, commit: &str) -> Result<(), GitTransactionError> {
        let result = (|| {
            run_git(
                Some(&self.canonical_worktree),
                None,
                [
                    OsStr::new("reset"),
                    OsStr::new("--hard"),
                    OsStr::new(commit),
                ],
            )?;
            run_git(
                Some(&self.canonical_worktree),
                None,
                [
                    OsStr::new("clean"),
                    OsStr::new("-d"),
                    OsStr::new("-f"),
                    OsStr::new("-x"),
                ],
            )?;
            sync_tree(&self.canonical_worktree)?;
            Ok(())
        })();
        result.map_err(|source| GitTransactionError::CanonicalWorktreeSync {
            commit: commit.into(),
            source: Box::new(source),
        })
    }

    fn journal_path(&self, batch_id: BatchId) -> PathBuf {
        self.journal_root.join(format!("{batch_id}.json"))
    }

    fn ensure_no_other_journal(&self, batch_id: BatchId) -> Result<(), GitTransactionError> {
        let expected_name = format!("{batch_id}.json");
        for entry in fs::read_dir(&self.journal_root).map_err(GitTransactionError::Io)? {
            let entry = entry.map_err(GitTransactionError::Io)?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| GitTransactionError::InvalidJournal)?;
            if name.ends_with(".json") && name != expected_name {
                return Err(GitTransactionError::UnfinishedTransaction);
            }
        }
        Ok(())
    }

    fn worktree_path(&self, batch_id: BatchId) -> PathBuf {
        self.worktree_root.join(format!("batch-{batch_id}"))
    }

    fn create_transaction_ref(
        &self,
        batch_id: BatchId,
        commit: &str,
    ) -> Result<(), GitTransactionError> {
        let transaction_ref = transaction_ref(batch_id);
        let zero = "0".repeat(commit.len());
        run_git(
            None,
            Some(&self.git_directory),
            [
                OsStr::new("update-ref"),
                OsStr::new(&transaction_ref),
                OsStr::new(commit),
                OsStr::new(&zero),
            ],
        )?;
        Ok(())
    }

    fn ensure_transaction_ref(
        &self,
        batch_id: BatchId,
        commit: &str,
    ) -> Result<(), GitTransactionError> {
        let transaction_ref = transaction_ref(batch_id);
        if self.resolve_optional_commit(&transaction_ref)?.as_deref() == Some(commit) {
            Ok(())
        } else {
            Err(GitTransactionError::JournalMismatch)
        }
    }

    fn compare_and_swap_official(
        &self,
        expected: &str,
        commit: &str,
    ) -> Result<(), GitTransactionError> {
        let output = git_command()
            .arg(git_directory_argument(&self.git_directory))
            .args(["update-ref", &self.official_ref, commit, expected])
            .output()
            .map_err(GitTransactionError::Io)?;
        if output.status.success() {
            Ok(())
        } else {
            let actual = self.resolve_commit(&self.official_ref)?;
            if actual != expected {
                Err(GitTransactionError::OfficialBranchChanged {
                    expected: expected.into(),
                    actual,
                })
            } else {
                Err(GitTransactionError::GitCommand {
                    arguments: ["update-ref", &self.official_ref, commit, expected]
                        .into_iter()
                        .map(OsString::from)
                        .collect(),
                    stderr: diagnostic(&output.stderr),
                })
            }
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

    fn remove_worktree(&self, worktree: &Path) -> Result<(), GitTransactionError> {
        if path_exists(worktree)? {
            let removal = run_git(
                None,
                Some(&self.git_directory),
                [
                    OsStr::new("worktree"),
                    OsStr::new("remove"),
                    OsStr::new("--force"),
                    worktree.as_os_str(),
                ],
            );
            if let Err(error) = removal {
                self.handle_failed_worktree_removal(worktree, error)?;
            }
        }
        run_git(
            None,
            Some(&self.git_directory),
            [
                OsStr::new("worktree"),
                OsStr::new("prune"),
                OsStr::new("--expire=now"),
            ],
        )?;
        sync_directory(&self.worktree_root).map_err(GitTransactionError::Io)
    }

    fn handle_failed_worktree_removal(
        &self,
        worktree: &Path,
        error: GitTransactionError,
    ) -> Result<(), GitTransactionError> {
        if self.is_registered_worktree(worktree)? {
            Err(error)
        } else {
            self.remove_unregistered_worktree(worktree)
        }
    }

    fn is_registered_worktree(&self, worktree: &Path) -> Result<bool, GitTransactionError> {
        let output = run_git(
            None,
            Some(&self.git_directory),
            [
                OsStr::new("worktree"),
                OsStr::new("list"),
                OsStr::new("--porcelain"),
                OsStr::new("-z"),
            ],
        )?;
        let expected = worktree.as_os_str().as_encoded_bytes();
        let canonical = fs::canonicalize(worktree).map_err(GitTransactionError::Io)?;
        let canonical = canonical.as_os_str().as_encoded_bytes();
        Ok(output.stdout.split(|byte| *byte == 0).any(|field| {
            field
                .strip_prefix(b"worktree ")
                .is_some_and(|registered| registered == expected || registered == canonical)
        }))
    }

    fn remove_unregistered_worktree(&self, worktree: &Path) -> Result<(), GitTransactionError> {
        let direct_child = worktree.parent() == Some(self.worktree_root.as_path());
        let valid_batch_name = worktree
            .file_name()
            .and_then(OsStr::to_str)
            .and_then(|name| name.strip_prefix("batch-"))
            .is_some_and(|identifier| {
                identifier
                    .parse::<BatchId>()
                    .is_ok_and(|batch_id| batch_id.to_string() == identifier)
            });
        if !direct_child || !valid_batch_name {
            return Err(GitTransactionError::InvalidDirectory(worktree.into()));
        }
        ensure_real_directory(worktree)?;
        fs::remove_dir_all(worktree).map_err(GitTransactionError::Io)?;
        sync_directory(&self.worktree_root).map_err(GitTransactionError::Io)
    }

    fn remove_transaction_ref(
        &self,
        batch_id: BatchId,
        expected_commit: Option<&str>,
    ) -> Result<(), GitTransactionError> {
        let transaction_ref = transaction_ref(batch_id);
        let Some(actual) = self.resolve_optional_commit(&transaction_ref)? else {
            return Ok(());
        };
        if expected_commit.is_some_and(|expected| expected != actual) {
            return Err(GitTransactionError::JournalMismatch);
        }
        run_git(
            None,
            Some(&self.git_directory),
            [
                OsStr::new("update-ref"),
                OsStr::new("-d"),
                OsStr::new(&transaction_ref),
                OsStr::new(&actual),
            ],
        )?;
        Ok(())
    }

    fn remove_preparing_ref(
        &self,
        batch_id: BatchId,
        base: &str,
    ) -> Result<(), GitTransactionError> {
        let transaction_ref = transaction_ref(batch_id);
        let Some(commit) = self.resolve_optional_commit(&transaction_ref)? else {
            return Ok(());
        };
        let parents = run_git(
            None,
            Some(&self.git_directory),
            [
                OsStr::new("rev-list"),
                OsStr::new("--parents"),
                OsStr::new("-n"),
                OsStr::new("1"),
                OsStr::new(&commit),
            ],
        )?;
        let parents = parse_text(&parents.stdout)?
            .split_ascii_whitespace()
            .collect::<Vec<_>>();
        let trailers = run_git(
            None,
            Some(&self.git_directory),
            [
                OsStr::new("show"),
                OsStr::new("-s"),
                OsStr::new("--format=%(trailers:key=Batch-ID,valueonly)"),
                OsStr::new(&commit),
            ],
        )?;
        if parents.as_slice() != [commit.as_str(), base]
            || parse_text(&trailers.stdout)? != batch_id.to_string()
        {
            return Err(GitTransactionError::JournalMismatch);
        }
        self.remove_transaction_ref(batch_id, Some(&commit))
    }

    fn resolve_optional_commit(
        &self,
        revision: &str,
    ) -> Result<Option<String>, GitTransactionError> {
        let expression = format!("{revision}^{{commit}}");
        let output = git_command()
            .arg(git_directory_argument(&self.git_directory))
            .args(["rev-parse", "--verify", "--quiet", &expression])
            .output()
            .map_err(GitTransactionError::Io)?;
        if output.status.success() {
            parse_object_id(&output.stdout).map(Some)
        } else if output.status.code() == Some(1) && output.stderr.is_empty() {
            Ok(None)
        } else {
            Err(GitTransactionError::GitCommand {
                arguments: ["rev-parse", "--verify", "--quiet", &expression]
                    .into_iter()
                    .map(OsString::from)
                    .collect(),
                stderr: diagnostic(&output.stderr),
            })
        }
    }
}

pub(crate) fn ensure_canonical_worktree_clean(
    canonical_worktree: &Path,
) -> Result<(), GitTransactionError> {
    let output = run_git(
        Some(canonical_worktree),
        None,
        [
            OsStr::new("status"),
            OsStr::new("--porcelain=v1"),
            OsStr::new("-z"),
            OsStr::new("--untracked-files=all"),
            OsStr::new("--ignored=matching"),
        ],
    )?;
    if output.stdout.is_empty() {
        Ok(())
    } else {
        Err(GitTransactionError::CanonicalWorktreeDirty)
    }
}

struct RepositoryWriter {
    _repository: File,
    _work_root: File,
}

impl Drop for RepositoryWriter {
    fn drop(&mut self) {
        let _ = self._work_root.unlock();
        let _ = self._repository.unlock();
    }
}

struct PreparedBatch {
    tree: String,
    successful: Vec<ClaimToken>,
    failures: Vec<RequestFailure>,
    moves: Vec<AppliedMove>,
}

struct TransactionHooks<F, H> {
    trial_build: F,
    before_publish: H,
}

fn continue_publication(_: &str, _: &str) -> Result<(), GitTransactionError> {
    Ok(())
}

#[cfg(test)]
fn accept_trial_build(_: &Path) -> Result<(), GitTransactionError> {
    Ok(())
}

#[cfg(test)]
fn interrupt_publication(_: &str, _: &str) -> Result<(), GitTransactionError> {
    Err(GitTransactionError::InvalidGitOutput)
}

fn transaction_ref(batch_id: BatchId) -> String {
    format!("refs/agent-knowledge/transactions/{batch_id}")
}

fn journal_claims(claims: &[ClaimedPackage]) -> Vec<JournalClaim> {
    claims
        .iter()
        .map(|claim| JournalClaim {
            request_id: claim.token().request_id(),
            attempt: claim.token().attempt().get(),
            acceptance_sequence: claim
                .package()
                .acceptance()
                .map_or(0, |acceptance| acceptance.sequence.get()),
        })
        .collect()
}

fn validate_journal_structure(
    journal: &TransactionJournal,
    batch_id: BatchId,
) -> Result<(), GitTransactionError> {
    if journal.schema_version != JOURNAL_SCHEMA_VERSION
        || journal.batch_id != batch_id
        || !valid_object_id(&journal.base_commit)
        || journal
            .claims
            .iter()
            .any(|claim| claim.attempt == 0 || claim.acceptance_sequence == 0)
        || journal
            .claims
            .windows(2)
            .any(|claims| claims[0].acceptance_sequence >= claims[1].acceptance_sequence)
    {
        return Err(GitTransactionError::JournalMismatch);
    }
    let (successful, failures) = match &journal.state {
        JournalState::Preparing => return Ok(()),
        JournalState::NoChanges { failures } => (&[][..], failures.as_slice()),
        JournalState::Committed {
            commit,
            successful,
            failures,
            ..
        } => {
            if !valid_object_id(commit) {
                return Err(GitTransactionError::JournalMismatch);
            }
            (successful.as_slice(), failures.as_slice())
        }
    };
    let claim_ids = journal
        .claims
        .iter()
        .map(|claim| claim.request_id)
        .collect::<HashSet<_>>();
    let mut outcomes = HashSet::with_capacity(successful.len() + failures.len());
    if successful
        .iter()
        .chain(failures.iter().map(|failure| &failure.request_id))
        .any(|request_id| !outcomes.insert(*request_id))
        || claim_ids.len() != journal.claims.len()
        || outcomes != claim_ids
    {
        return Err(GitTransactionError::JournalMismatch);
    }
    Ok(())
}

fn validate_journal_claims(
    journal: &TransactionJournal,
    claims: &[ClaimedPackage],
) -> Result<(), GitTransactionError> {
    if journal.claims == journal_claims(claims) {
        Ok(())
    } else {
        Err(GitTransactionError::JournalMismatch)
    }
}

fn validate_worker_identity(
    journal: &TransactionJournal,
    worker: &WorkerSession,
) -> Result<(), GitTransactionError> {
    let identity = worker
        .queue_identity()
        .map_err(GitTransactionError::Queue)?;
    if identity == journal.queue_identity {
        Ok(())
    } else {
        Err(GitTransactionError::JournalMismatch)
    }
}

fn outcome_from_journal(
    journal: &TransactionJournal,
) -> Result<BatchCommitOutcome, GitTransactionError> {
    let (commit, successful, failures) = match &journal.state {
        JournalState::Preparing => return Err(GitTransactionError::JournalState),
        JournalState::NoChanges { failures } => (None, &[][..], failures.as_slice()),
        JournalState::Committed {
            commit,
            successful,
            failures,
            ..
        } => (
            Some(commit.as_str()),
            successful.as_slice(),
            failures.as_slice(),
        ),
    };
    let token = |request_id| {
        let claim = journal
            .claims
            .iter()
            .find(|claim| claim.request_id == request_id)
            .ok_or(GitTransactionError::JournalMismatch)?;
        let attempt = NonZeroU32::new(claim.attempt).ok_or(GitTransactionError::JournalMismatch)?;
        Ok(ClaimToken::from_durable_record(
            request_id,
            journal.batch_id,
            attempt,
        ))
    };
    let successful = successful
        .iter()
        .map(|request_id| token(*request_id))
        .collect::<Result<Vec<_>, _>>()?;
    let failures = failures
        .iter()
        .map(|failure| {
            Ok(RequestFailure {
                token: token(failure.request_id)?,
                error_code: failure.error_code,
            })
        })
        .collect::<Result<Vec<_>, GitTransactionError>>()?;
    match commit {
        Some(commit) => Ok(BatchCommitOutcome::Committed {
            commit: commit.into(),
            successful,
            failures,
        }),
        None if successful.is_empty() => Ok(BatchCommitOutcome::NoChanges { failures }),
        None => Err(GitTransactionError::JournalMismatch),
    }
}

fn outcome_parts(
    outcome: &BatchCommitOutcome,
) -> (Option<&str>, &[ClaimToken], Vec<(ClaimToken, ErrorCode)>) {
    match outcome {
        BatchCommitOutcome::NoChanges { failures } => (
            None,
            &[],
            failures
                .iter()
                .map(|failure| (failure.token(), failure.error_code()))
                .collect(),
        ),
        BatchCommitOutcome::Committed {
            commit,
            successful,
            failures,
        } => (
            Some(commit),
            successful,
            failures
                .iter()
                .map(|failure| (failure.token(), failure.error_code()))
                .collect(),
        ),
    }
}

fn read_journal(path: &Path) -> Result<Option<TransactionJournal>, GitTransactionError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(GitTransactionError::Io(error)),
    };
    if !metadata.file_type().is_file() || metadata.len() > MAXIMUM_JOURNAL_BYTES {
        return Err(GitTransactionError::InvalidJournal);
    }
    let capacity =
        usize::try_from(metadata.len()).map_err(|_| GitTransactionError::InvalidJournal)?;
    let mut bytes = Vec::with_capacity(capacity);
    File::open(path)
        .and_then(|file| file.take(MAXIMUM_JOURNAL_BYTES + 1).read_to_end(&mut bytes))
        .map_err(GitTransactionError::Io)?;
    if bytes.len() as u64 > MAXIMUM_JOURNAL_BYTES {
        return Err(GitTransactionError::InvalidJournal);
    }
    decode_journal(&bytes).map(Some)
}

fn decode_journal(bytes: &[u8]) -> Result<TransactionJournal, GitTransactionError> {
    let stored: StoredTransactionJournal =
        serde_json::from_slice(bytes).map_err(|_| GitTransactionError::InvalidJournal)?;
    match stored {
        StoredTransactionJournal::Current(journal)
            if journal.schema_version == JOURNAL_SCHEMA_VERSION =>
        {
            Ok(journal)
        }
        StoredTransactionJournal::Previous(journal)
            if journal.schema_version == PREVIOUS_JOURNAL_SCHEMA_VERSION =>
        {
            Ok(TransactionJournal {
                schema_version: JOURNAL_SCHEMA_VERSION,
                batch_id: journal.batch_id,
                queue_identity: journal.queue_identity,
                base_commit: journal.base_commit,
                claims: journal.claims,
                claim_failures: 0,
                state: journal.state,
            })
        }
        _ => Err(GitTransactionError::InvalidJournal),
    }
}

fn write_journal(path: &Path, journal: &TransactionJournal) -> Result<(), GitTransactionError> {
    let parent = path.parent().ok_or(GitTransactionError::InvalidJournal)?;
    let temporary = parent.join(format!(".journal-{}.tmp", Ulid::generate()));
    let mut bytes = serde_json::to_vec(journal).map_err(|_| GitTransactionError::InvalidJournal)?;
    bytes.push(b'\n');
    if bytes.len() as u64 > MAXIMUM_JOURNAL_BYTES {
        return Err(GitTransactionError::InvalidJournal);
    }
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        sync_directory(parent)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result.map_err(GitTransactionError::Io)
}

fn remove_journal(path: &Path) -> Result<(), GitTransactionError> {
    fs::remove_file(path).map_err(GitTransactionError::Io)?;
    let parent = path.parent().ok_or(GitTransactionError::InvalidJournal)?;
    sync_directory(parent).map_err(GitTransactionError::Io)
}

fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

fn sync_tree(path: &Path) -> Result<(), GitTransactionError> {
    for entry in fs::read_dir(path).map_err(GitTransactionError::Io)? {
        let entry = entry.map_err(GitTransactionError::Io)?;
        let entry_path = entry.path();
        let metadata = fs::symlink_metadata(&entry_path).map_err(GitTransactionError::Io)?;
        if metadata.file_type().is_dir() {
            sync_tree(&entry_path)?;
        } else if metadata.file_type().is_file() {
            File::open(&entry_path)
                .and_then(|file| file.sync_all())
                .map_err(GitTransactionError::Io)?;
        } else {
            return Err(GitTransactionError::InvalidDirectory(entry_path));
        }
    }
    sync_directory(path).map_err(GitTransactionError::Io)
}

fn validate_batch_claims(
    worker: &mut WorkerSession,
    batch_id: BatchId,
    claims: &[ClaimedPackage],
) -> Result<(), GitTransactionError> {
    let mut request_ids = HashSet::with_capacity(claims.len());
    let mut previous_order = None;
    for claim in claims {
        let token = claim.token();
        if token.batch_id() != batch_id || !request_ids.insert(token.request_id()) {
            return Err(GitTransactionError::InvalidClaims);
        }
        worker
            .validate_claimed(claim)
            .map_err(GitTransactionError::Queue)?;
        let acceptance = claim
            .package()
            .acceptance()
            .ok_or(GitTransactionError::InvalidClaims)?;
        let order = acceptance.sequence;
        if previous_order.is_some_and(|previous| previous >= order) {
            return Err(GitTransactionError::InvalidClaims);
        }
        previous_order = Some(order);
    }
    Ok(())
}

fn reset_worktree(worktree: &Path, tree: &str) -> Result<(), GitTransactionError> {
    run_git(
        Some(worktree),
        None,
        [
            OsStr::new("read-tree"),
            OsStr::new("--reset"),
            OsStr::new("-u"),
            OsStr::new(tree),
        ],
    )?;
    run_git(
        Some(worktree),
        None,
        [
            OsStr::new("clean"),
            OsStr::new("-d"),
            OsStr::new("-f"),
            OsStr::new("-x"),
        ],
    )?;
    Ok(())
}

pub(crate) fn ensure_real_directory(path: &Path) -> Result<(), GitTransactionError> {
    let metadata = fs::symlink_metadata(path).map_err(GitTransactionError::Io)?;
    if !metadata.file_type().is_dir() {
        return Err(GitTransactionError::InvalidDirectory(path.into()));
    }
    Ok(())
}

fn path_exists(path: &Path) -> Result<bool, GitTransactionError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(GitTransactionError::Io(error)),
    }
}

fn ensure_or_create_real_directory(path: &Path) -> Result<(), GitTransactionError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(()),
        Ok(_) => Err(GitTransactionError::InvalidDirectory(path.into())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if let Err(error) = fs::create_dir(path)
                && error.kind() != io::ErrorKind::AlreadyExists
            {
                return Err(GitTransactionError::Io(error));
            }
            ensure_real_directory(path)?;
            let parent = path
                .parent()
                .ok_or_else(|| GitTransactionError::InvalidDirectory(path.into()))?;
            sync_directory(parent).map_err(GitTransactionError::Io)
        }
        Err(error) => Err(GitTransactionError::Io(error)),
    }
}

pub(crate) fn open_stable_directory(
    path: &Path,
) -> Result<(Arc<File>, PathBuf), GitTransactionError> {
    let handle = Arc::new(File::open(path).map_err(GitTransactionError::Io)?);
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let stable = PathBuf::from(format!(
            "/proc/{}/fd/{}",
            std::process::id(),
            handle.as_raw_fd()
        ));
        fs::metadata(&stable).map_err(GitTransactionError::Io)?;
        Ok((handle, stable))
    }
    #[cfg(not(target_os = "linux"))]
    {
        Ok((handle, path.to_path_buf()))
    }
}

fn lock_root_paths(
    stable_repository: &Path,
    repository_path: &Path,
    stable_work_root: &Path,
    work_root_path: &Path,
) -> Result<RepositoryWriter, GitTransactionError> {
    let repository = File::open(stable_repository).map_err(GitTransactionError::Io)?;
    match repository.try_lock() {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => {
            return Err(GitTransactionError::RepositoryBusy(
                repository_path.to_path_buf(),
            ));
        }
        Err(TryLockError::Error(error)) => return Err(GitTransactionError::Io(error)),
    }
    let work_root = match File::open(stable_work_root) {
        Ok(work_root) => work_root,
        Err(error) => {
            let _ = repository.unlock();
            return Err(GitTransactionError::Io(error));
        }
    };
    match work_root.try_lock() {
        Ok(()) => Ok(RepositoryWriter {
            _repository: repository,
            _work_root: work_root,
        }),
        Err(TryLockError::WouldBlock) => {
            let _ = repository.unlock();
            Err(GitTransactionError::RepositoryBusy(
                work_root_path.to_path_buf(),
            ))
        }
        Err(TryLockError::Error(error)) => {
            let _ = repository.unlock();
            Err(GitTransactionError::Io(error))
        }
    }
}

fn validate_pinned_directory(configured: &Path, pinned: &File) -> Result<(), GitTransactionError> {
    let configured_metadata = fs::symlink_metadata(configured).map_err(GitTransactionError::Io)?;
    let pinned_metadata = pinned.metadata().map_err(GitTransactionError::Io)?;
    if !configured_metadata.file_type().is_dir()
        || !same_metadata(&configured_metadata, &pinned_metadata)
    {
        return Err(GitTransactionError::RepositoryBindingMismatch);
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
    left.file_type() == right.file_type()
        && left.len() == right.len()
        && left.modified().ok() == right.modified().ok()
}

fn same_directory(left: &Path, right: &Path) -> Result<bool, GitTransactionError> {
    let left = fs::metadata(left).map_err(GitTransactionError::Io)?;
    let right = fs::metadata(right).map_err(GitTransactionError::Io)?;
    Ok(left.is_dir() && right.is_dir() && same_metadata(&left, &right))
}

fn ensure_binding(path: &Path, expected: &[u8]) -> Result<(), GitTransactionError> {
    if expected.len() > MAXIMUM_BINDING_BYTES {
        return Err(GitTransactionError::RepositoryBindingMismatch);
    }
    match fs::symlink_metadata(path) {
        Ok(_) => validate_binding(path, expected),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
                .map_err(GitTransactionError::Io)?;
            file.write_all(expected).map_err(GitTransactionError::Io)?;
            file.sync_all().map_err(GitTransactionError::Io)?;
            let parent = path
                .parent()
                .ok_or(GitTransactionError::RepositoryBindingMismatch)?;
            sync_directory(parent).map_err(GitTransactionError::Io)
        }
        Err(error) => Err(GitTransactionError::Io(error)),
    }
}

fn validate_binding(path: &Path, expected: &[u8]) -> Result<(), GitTransactionError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            GitTransactionError::RepositoryBindingMismatch
        } else {
            GitTransactionError::Io(error)
        }
    })?;
    if !metadata.file_type().is_file() || metadata.len() > MAXIMUM_BINDING_BYTES as u64 {
        return Err(GitTransactionError::RepositoryBindingMismatch);
    }
    let mut actual = Vec::with_capacity(metadata.len() as usize);
    File::open(path)
        .and_then(|file| {
            file.take(MAXIMUM_BINDING_BYTES as u64 + 1)
                .read_to_end(&mut actual)
        })
        .map_err(GitTransactionError::Io)?;
    if actual == expected {
        Ok(())
    } else {
        Err(GitTransactionError::RepositoryBindingMismatch)
    }
}

fn repository_binding(
    directories: &[(&Path, &File)],
    official_ref: &str,
) -> Result<Vec<u8>, GitTransactionError> {
    let mut binding = b"agent-knowledge-repository-binding-v2\0".to_vec();
    for (configured_path, handle) in directories {
        append_directory_binding(&mut binding, configured_path, handle)?;
    }
    binding.extend_from_slice(official_ref.as_bytes());
    Ok(binding)
}

fn append_directory_binding(
    binding: &mut Vec<u8>,
    configured_path: &Path,
    handle: &File,
) -> Result<(), GitTransactionError> {
    binding.extend_from_slice(configured_path.as_os_str().as_encoded_bytes());
    binding.push(0);
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = handle.metadata().map_err(GitTransactionError::Io)?;
        binding.extend_from_slice(&metadata.dev().to_le_bytes());
        binding.extend_from_slice(&metadata.ino().to_le_bytes());
    }
    binding.push(0);
    Ok(())
}

pub(crate) fn validate_repository_layout(
    git_directory: &Path,
    canonical_worktree: &Path,
    official_ref: &str,
) -> Result<(), GitTransactionError> {
    let bare = run_git(
        None,
        Some(git_directory),
        [OsStr::new("rev-parse"), OsStr::new("--is-bare-repository")],
    )?;
    if parse_text(&bare.stdout)? != "true" {
        return Err(GitTransactionError::RepositoryNotBare);
    }
    let inside = run_git(
        Some(canonical_worktree),
        None,
        [OsStr::new("rev-parse"), OsStr::new("--is-inside-work-tree")],
    )?;
    if parse_text(&inside.stdout)? != "true" {
        return Err(GitTransactionError::CanonicalWorktreeMismatch);
    }
    let top_level = run_git(
        Some(canonical_worktree),
        None,
        [
            OsStr::new("rev-parse"),
            OsStr::new("--path-format=absolute"),
            OsStr::new("--show-toplevel"),
        ],
    )?;
    let top_level =
        fs::canonicalize(parse_git_path(&top_level.stdout)?).map_err(GitTransactionError::Io)?;
    if !same_directory(&top_level, canonical_worktree)? {
        return Err(GitTransactionError::CanonicalWorktreeMismatch);
    }
    let common_directory = run_git(
        Some(canonical_worktree),
        None,
        [
            OsStr::new("rev-parse"),
            OsStr::new("--path-format=absolute"),
            OsStr::new("--git-common-dir"),
        ],
    )?;
    let common_directory = fs::canonicalize(parse_git_path(&common_directory.stdout)?)
        .map_err(GitTransactionError::Io)?;
    if !same_directory(&common_directory, git_directory)? {
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
    Ok(())
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

fn commit_tree(
    git_directory: &Path,
    identity: &GitIdentity,
    tree: &str,
    parent: &str,
    message: &str,
) -> Result<String, GitTransactionError> {
    let name = format!("user.name={}", identity.name);
    let email = format!("user.email={}", identity.email);
    let output = run_git_with_input(
        None,
        Some(git_directory),
        [
            OsStr::new("-c"),
            OsStr::new(&name),
            OsStr::new("-c"),
            OsStr::new(&email),
            OsStr::new("commit-tree"),
            OsStr::new(tree),
            OsStr::new("-p"),
            OsStr::new(parent),
        ],
        message.as_bytes(),
    )?;
    parse_object_id(&output.stdout)
}

#[derive(Clone, Copy)]
struct FileStats {
    added: usize,
    modified: usize,
    deleted: usize,
}

fn staged_stats(
    worktree: &Path,
    base: &str,
    moves: &[AppliedMove],
) -> Result<FileStats, GitTransactionError> {
    let final_moves = reduce_moves(moves)?;
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
    let mut added = HashSet::new();
    let mut deleted = Vec::new();
    let mut modified = 0;
    while let Some(status) = parts.next() {
        if status.is_empty() {
            break;
        }
        let Some(path) = parts.next() else {
            return Err(GitTransactionError::InvalidGitOutput);
        };
        let path = std::str::from_utf8(path)
            .map(PathBuf::from)
            .map_err(|_| GitTransactionError::InvalidGitOutput)?;
        if path.as_os_str().is_empty() {
            return Err(GitTransactionError::InvalidGitOutput);
        }
        match status {
            b"A" => {
                if !added.insert(path) {
                    return Err(GitTransactionError::InvalidGitOutput);
                }
            }
            b"M" => modified += 1,
            b"D" => deleted.push(path),
            _ => return Err(GitTransactionError::InvalidGitOutput),
        }
    }
    for source in deleted {
        let destination = moved_file_destination(&source, &final_moves)
            .ok_or(GitTransactionError::PhysicalDeletion)?;
        if destination == source || !added.remove(&destination) {
            return Err(GitTransactionError::PhysicalDeletion);
        }
        modified += 1;
    }
    Ok(FileStats {
        added: added.len(),
        modified,
        deleted: 0,
    })
}

fn reduce_moves(moves: &[AppliedMove]) -> Result<HashMap<PathBuf, PathBuf>, GitTransactionError> {
    let mut origins = HashMap::<PathBuf, PathBuf>::with_capacity(moves.len());
    for applied_move in moves {
        let origin = origins
            .remove(&applied_move.source)
            .unwrap_or_else(|| applied_move.source.clone());
        if origins
            .insert(applied_move.destination.clone(), origin)
            .is_some()
        {
            return Err(GitTransactionError::InvalidGitOutput);
        }
    }
    let mut final_moves = HashMap::with_capacity(origins.len());
    for (destination, origin) in origins {
        if final_moves.insert(origin, destination).is_some() {
            return Err(GitTransactionError::InvalidGitOutput);
        }
    }
    Ok(final_moves)
}

fn moved_file_destination(
    source: &Path,
    final_moves: &HashMap<PathBuf, PathBuf>,
) -> Option<PathBuf> {
    let mut ancestor = Some(source);
    while let Some(candidate) = ancestor {
        if let Some(destination) = final_moves.get(candidate) {
            let suffix = source.strip_prefix(candidate).ok()?;
            return Some(destination.join(suffix));
        }
        ancestor = candidate.parent();
    }
    None
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
        "\nRequests: {}\nFiles-Added: {}\nFiles-Modified: {}\nFiles-Deleted: {}\n\nBatch-ID: {batch_id}\n",
        successful.len(),
        stats.added,
        stats.modified,
        stats.deleted
    ));
    for token in successful {
        message.push_str(&format!("Knowledge-Request: {}\n", token.request_id()));
    }
    message
}

pub(crate) fn run_git<I, S>(
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
    let mut command = git_command();
    if let Some(working_directory) = working_directory {
        command.arg("-C").arg(working_directory);
    }
    if let Some(git_directory) = git_directory {
        command.arg(git_directory_argument(git_directory));
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

fn git_directory_argument(git_directory: &Path) -> OsString {
    let mut argument = OsString::from("--git-dir=");
    argument.push(git_directory.as_os_str());
    argument
}

fn git_command() -> Command {
    let mut command = Command::new("git");
    for (name, _) in std::env::vars_os() {
        if name.as_encoded_bytes().starts_with(b"GIT_") {
            command.env_remove(name);
        }
    }
    command
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .args([
            "-c",
            "core.fsync=all",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.autocrlf=false",
            "-c",
            "core.eol=lf",
            "-c",
            "commit.gpgSign=false",
            "-c",
            "tag.gpgSign=false",
        ]);
    command
}

pub(crate) fn validate_local_git_config(git_directory: &Path) -> Result<(), GitTransactionError> {
    let output = run_git(
        None,
        Some(git_directory),
        [
            OsStr::new("config"),
            OsStr::new("--local"),
            OsStr::new("--name-only"),
            OsStr::new("--null"),
            OsStr::new("--list"),
        ],
    )?;
    for key in output.stdout.split(|byte| *byte == 0) {
        if key.is_empty() {
            continue;
        }
        let key = std::str::from_utf8(key).map_err(|_| GitTransactionError::UnsafeGitConfig)?;
        let allowed = matches!(
            key,
            "core.repositoryformatversion"
                | "core.filemode"
                | "core.bare"
                | "core.logallrefupdates"
                | "extensions.objectformat"
        ) || safe_remote_or_branch_config(key);
        if !allowed {
            return Err(GitTransactionError::UnsafeGitConfig);
        }
    }
    require_local_boolean(git_directory, "core.bare", true)?;
    require_local_boolean(git_directory, "core.filemode", true)?;
    Ok(())
}

fn require_local_boolean(
    git_directory: &Path,
    key: &str,
    expected: bool,
) -> Result<(), GitTransactionError> {
    let output = run_git(
        None,
        Some(git_directory),
        [
            OsStr::new("config"),
            OsStr::new("--local"),
            OsStr::new("--type=bool"),
            OsStr::new("--get"),
            OsStr::new(key),
        ],
    )?;
    if parse_text(&output.stdout)? == if expected { "true" } else { "false" } {
        Ok(())
    } else {
        Err(GitTransactionError::UnsafeGitConfig)
    }
}

fn safe_remote_or_branch_config(key: &str) -> bool {
    if let Some(value) = key.strip_prefix("remote.")
        && let Some((name, field)) = value.rsplit_once('.')
    {
        return !name.is_empty() && matches!(field, "url" | "pushurl" | "fetch" | "mirror");
    }
    if let Some(value) = key.strip_prefix("branch.")
        && let Some((name, field)) = value.rsplit_once('.')
    {
        return !name.is_empty() && matches!(field, "remote" | "merge");
    }
    false
}

fn untracked_files(worktree: &Path) -> Result<Vec<PathBuf>, GitTransactionError> {
    let mut paths = Vec::new();
    let mut seen = HashSet::new();
    for arguments in [
        &["ls-files", "--others", "-z", "--exclude-standard"][..],
        &[
            "ls-files",
            "--others",
            "--ignored",
            "-z",
            "--exclude-standard",
        ][..],
    ] {
        let output = run_git(Some(worktree), None, arguments.iter().map(OsStr::new))?;
        for path in output.stdout.split(|byte| *byte == 0) {
            if path.is_empty() {
                continue;
            }
            let path =
                std::str::from_utf8(path).map_err(|_| GitTransactionError::InvalidGitOutput)?;
            let path = PathBuf::from(path);
            if seen.insert(path.clone()) {
                paths.push(path);
            }
        }
    }
    Ok(paths)
}

fn resolve_tree_path(
    git_directory: &Path,
    tree: &str,
    path: &Path,
) -> Result<Option<String>, GitTransactionError> {
    let output = run_git(
        None,
        Some(git_directory),
        [
            OsStr::new("ls-tree"),
            OsStr::new("-z"),
            OsStr::new("--full-tree"),
            OsStr::new(tree),
            OsStr::new("--"),
            path.as_os_str(),
        ],
    )?;
    let records = output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|record| !record.is_empty())
        .collect::<Vec<_>>();
    if records.is_empty() {
        return Ok(None);
    }
    if records.len() != 1 {
        return Err(GitTransactionError::InvalidGitOutput);
    }
    let record =
        std::str::from_utf8(records[0]).map_err(|_| GitTransactionError::InvalidGitOutput)?;
    let (metadata, found_path) = record
        .split_once('\t')
        .ok_or(GitTransactionError::InvalidGitOutput)?;
    if Path::new(found_path) != path {
        return Err(GitTransactionError::InvalidGitOutput);
    }
    let mut metadata = metadata.split_ascii_whitespace();
    let _mode = metadata.next();
    let kind = metadata.next();
    let object = metadata.next();
    if kind != Some("blob") || metadata.next().is_some() {
        return Err(GitTransactionError::InvalidGitOutput);
    }
    object
        .filter(|object| valid_object_id(object))
        .map(|object| Some(object.into()))
        .ok_or(GitTransactionError::InvalidGitOutput)
}

fn hash_worktree_file(worktree: &Path, path: &Path) -> Result<String, GitTransactionError> {
    let output = run_git(
        Some(worktree),
        None,
        [
            OsStr::new("hash-object"),
            OsStr::new("--no-filters"),
            OsStr::new("--"),
            path.as_os_str(),
        ],
    )?;
    parse_object_id(&output.stdout)
}

pub(crate) fn ensure_supported_git() -> Result<(), GitTransactionError> {
    let output = git_command()
        .arg("--version")
        .output()
        .map_err(GitTransactionError::Io)?;
    if !output.status.success() {
        return Err(GitTransactionError::GitCommand {
            arguments: vec![OsString::from("--version")],
            stderr: diagnostic(&output.stderr),
        });
    }
    let version = parse_git_version(&output.stdout)?;
    if version.0 > 2 || (version.0 == 2 && version.1 >= 36) {
        Ok(())
    } else {
        Err(GitTransactionError::UnsupportedGitVersion {
            found: String::from_utf8_lossy(&output.stdout).trim().into(),
        })
    }
}

fn parse_git_version(output: &[u8]) -> Result<(u64, u64), GitTransactionError> {
    let version = std::str::from_utf8(output)
        .map_err(|_| GitTransactionError::InvalidGitOutput)?
        .trim()
        .strip_prefix("git version ")
        .ok_or(GitTransactionError::InvalidGitOutput)?;
    let mut components = version.split('.');
    let major = components
        .next()
        .and_then(|value| value.parse().ok())
        .ok_or(GitTransactionError::InvalidGitOutput)?;
    let minor = components
        .next()
        .and_then(|value| value.parse().ok())
        .ok_or(GitTransactionError::InvalidGitOutput)?;
    Ok((major, minor))
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

fn parse_git_path(output: &[u8]) -> Result<PathBuf, GitTransactionError> {
    let value = output
        .strip_suffix(b"\n")
        .ok_or(GitTransactionError::InvalidGitOutput)?;
    if value.is_empty() || value.contains(&b'\n') {
        return Err(GitTransactionError::InvalidGitOutput);
    }
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;
        Ok(PathBuf::from(OsString::from_vec(value.to_vec())))
    }
    #[cfg(not(unix))]
    {
        let value =
            std::str::from_utf8(value).map_err(|_| GitTransactionError::InvalidGitOutput)?;
        Ok(PathBuf::from(value))
    }
}

pub(crate) fn parse_object_id(output: &[u8]) -> Result<String, GitTransactionError> {
    let value = parse_text(output)?;
    if !valid_object_id(value) {
        return Err(GitTransactionError::InvalidGitOutput);
    }
    Ok(value.into())
}

fn valid_object_id(value: &str) -> bool {
    matches!(value.len(), 40 | 64)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn diagnostic(stderr: &[u8]) -> String {
    let limit = stderr.len().min(MAXIMUM_DIAGNOSTIC_BYTES);
    String::from_utf8_lossy(&stderr[..limit]).into_owned()
}

/// A Git worktree transaction or publication failure.
#[derive(Debug)]
pub enum GitTransactionError {
    /// The installed Git predates required fsync configuration support.
    UnsupportedGitVersion {
        /// Version string reported by Git.
        found: String,
    },
    /// The caller's trial static-site build rejected the prepared worktree.
    TrialBuildFailed,
    /// The trial build changed its supposedly read-only input worktree.
    TrialBuildMutatedWorktree,
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
    /// Another process holds the repository-scoped writer lock.
    RepositoryBusy(PathBuf),
    /// The repository was already bound to another writer configuration.
    RepositoryBindingMismatch,
    /// Repository-local Git configuration contained a non-allowlisted key.
    UnsafeGitConfig,
    /// A required directory was a link or non-directory entry.
    InvalidDirectory(PathBuf),
    /// The batch contained no claims.
    EmptyBatch,
    /// Claims were duplicated, unordered, or owned by another batch.
    InvalidClaims,
    /// The canonical worktree contains changes outside Repository Worker control.
    CanonicalWorktreeDirty,
    /// The deterministic batch worktree path already existed.
    WorktreeAlreadyExists(PathBuf),
    /// A durable transaction journal was malformed or exceeded its bound.
    InvalidJournal,
    /// A durable transaction journal did not describe the supplied batch.
    JournalMismatch,
    /// A required durable transaction journal was absent.
    JournalMissing,
    /// A transaction journal was not in the committed state.
    JournalState,
    /// Queue reconciliation or release activation has not completed.
    PublicationIncomplete,
    /// A committed transaction must be recovered before it can be reconciled.
    TransactionRequiresRecovery {
        /// Batch whose durable result must be recovered.
        batch_id: BatchId,
    },
    /// Another durable transaction must be reconciled before a new batch.
    UnfinishedTransaction,
    /// A request failed for a non-isolatable reason.
    Apply {
        /// Request being applied.
        request_id: RequestId,
        /// Underlying apply failure.
        source: ApplyError,
    },
    /// Live queue ownership validation failed.
    Queue(WorkerQueueError),
    /// Active release validation failed.
    Release(Box<ReleaseError>),
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
    /// A transaction attempted a physical content deletion.
    PhysicalDeletion,
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
            Self::UnsupportedGitVersion { found } => {
                write!(formatter, "Git 2.36 or newer is required; found `{found}`")
            }
            Self::TrialBuildFailed => formatter.write_str("trial static-site build failed"),
            Self::TrialBuildMutatedWorktree => {
                formatter.write_str("trial static-site build changed its input worktree")
            }
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
            Self::RepositoryBusy(path) => write!(
                formatter,
                "repository writer lock `{}` is already held",
                path.display()
            ),
            Self::RepositoryBindingMismatch => {
                formatter.write_str("repository is bound to a different writer configuration")
            }
            Self::UnsafeGitConfig => {
                formatter.write_str("repository-local Git configuration is not allowlisted")
            }
            Self::InvalidDirectory(path) => {
                write!(formatter, "`{}` must be a real directory", path.display())
            }
            Self::EmptyBatch => formatter.write_str("Git transaction batch must not be empty"),
            Self::InvalidClaims => {
                formatter.write_str("Git transaction claims are unordered or inconsistently owned")
            }
            Self::CanonicalWorktreeDirty => {
                formatter.write_str("canonical worktree contains uncommitted changes")
            }
            Self::WorktreeAlreadyExists(path) => {
                write!(
                    formatter,
                    "transaction worktree `{}` exists",
                    path.display()
                )
            }
            Self::InvalidJournal => formatter.write_str("transaction journal is invalid"),
            Self::JournalMismatch => {
                formatter.write_str("transaction journal does not match the supplied batch")
            }
            Self::JournalMissing => formatter.write_str("transaction journal is missing"),
            Self::JournalState => {
                formatter.write_str("transaction journal is not ready for finalization")
            }
            Self::PublicationIncomplete => formatter.write_str(
                "queue reconciliation and active release do not complete this transaction",
            ),
            Self::TransactionRequiresRecovery { batch_id } => {
                write!(
                    formatter,
                    "transaction batch `{batch_id}` requires journal recovery"
                )
            }
            Self::UnfinishedTransaction => {
                formatter.write_str("another durable transaction is not finalized")
            }
            Self::Apply { request_id, source } => {
                write!(
                    formatter,
                    "request `{request_id}` could not be isolated: {source}"
                )
            }
            Self::Queue(error) => write!(formatter, "claim ownership validation failed: {error}"),
            Self::Release(error) => write!(formatter, "active release validation failed: {error}"),
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
            Self::PhysicalDeletion => {
                formatter.write_str("Git transaction attempted a physical content deletion")
            }
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
            Self::Queue(error) => Some(error),
            Self::Release(error) => Some(error),
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

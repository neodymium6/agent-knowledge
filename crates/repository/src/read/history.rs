//! Bounded reads of immutable Git objects reachable from the official branch.

use super::*;

/// An exact historical Markdown document and its validated identity.
#[derive(Clone, Debug)]
pub struct HistoricalDocument {
    pub record: DocumentRecord,
    pub markdown: String,
}

/// A pinned official ancestry. Reading history never locks the canonical tree.
pub struct HistoryReader {
    git_directory: PathBuf,
    _git_root_handle: Arc<File>,
    anchor: String,
    policy: ContentPolicy,
    bytes_read: u64,
    entries_read: usize,
}

impl CommittedStore {
    /// Pins an official commit and bounds cumulative historical inspection work.
    ///
    /// # Errors
    /// Returns an error for invalid storage, publication contention, or expiry.
    pub fn history_reader(
        &self,
        policy: ContentPolicy,
    ) -> Result<HistoryReader, CommittedReadError> {
        let anchor = self.current_commit_until(policy.scan_deadline)?;
        Ok(HistoryReader {
            git_directory: self.git_directory.clone(),
            _git_root_handle: Arc::clone(&self.git_root_handle),
            anchor,
            policy,
            bytes_read: 0,
            entries_read: 0,
        })
    }
}

impl HistoryReader {
    /// Returns the official commit pinned when this reader was created.
    #[must_use]
    pub fn anchor(&self) -> &str {
        &self.anchor
    }

    /// Validates a full object ID as an ancestor of the selected official commit.
    ///
    /// # Errors
    /// Rejects expressions, abbreviated IDs, and unpublished commits.
    pub fn validate_commit(&self, commit: &str) -> Result<(), CommittedReadError> {
        if commit.len() != self.anchor.len()
            || !commit
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(CommittedReadError::InvalidHistorySelector);
        }
        self.git(["merge-base", "--is-ancestor", commit, &self.anchor], 0)
            .map_err(|error| match error {
                CommittedReadError::Repository(ref source)
                    if matches!(source.as_ref(), GitTransactionError::GitCommand { .. }) =>
                {
                    CommittedReadError::InvalidHistorySelector
                }
                _ => error,
            })?;
        Ok(())
    }

    /// Restricts later reads to an earlier official snapshot, for stable pagination.
    ///
    /// # Errors
    /// Rejects a selector outside the current official history.
    pub fn restrict_to(&mut self, commit: &str) -> Result<(), CommittedReadError> {
        self.validate_commit(commit)?;
        self.anchor = commit.to_owned();
        Ok(())
    }

    /// Lists at most `maximum` first-parent commits, starting at the cursor.
    ///
    /// # Errors
    /// Rejects an invalid cursor, zero/excessive bounds, or expired work.
    pub fn commits(
        &self,
        cursor: Option<&str>,
        maximum: usize,
    ) -> Result<Vec<String>, CommittedReadError> {
        if maximum == 0 || maximum > 102 {
            return Err(CommittedReadError::InvalidResultLimit);
        }
        let start = cursor.unwrap_or(&self.anchor);
        self.validate_commit(start)?;
        let count = format!("--max-count={maximum}");
        let output = self.git(
            ["rev-list", "--first-parent", &count, start, "--"],
            maximum * 66,
        )?;
        let text =
            std::str::from_utf8(&output).map_err(|_| CommittedReadError::InvalidHistorySelector)?;
        Ok(text.lines().map(str::to_owned).collect())
    }

    /// Resolves identity from Markdown front matter, including across moves.
    ///
    /// # Errors
    /// Enforces cumulative tree/Markdown budgets, validates canonical metadata,
    /// and rejects duplicate identities or invalid Git tree entries.
    pub fn document(
        &mut self,
        commit: &str,
        id: DocumentId,
    ) -> Result<Option<HistoricalDocument>, CommittedReadError> {
        self.validate_commit(commit)?;
        let listing = self.git(["ls-tree", "-r", "-z", commit, "--"], 16 * 1024 * 1024)?;
        let mut found: Option<HistoricalDocument> = None;
        for entry in listing
            .split(|byte| *byte == 0)
            .filter(|entry| !entry.is_empty())
        {
            check_operation_deadline(self.policy.scan_deadline)?;
            self.entries_read = self.entries_read.saturating_add(1);
            if self.entries_read > self.policy.maximum_entry_count {
                return Err(CommittedReadError::SearchDocumentLimitExceeded {
                    maximum: self.policy.maximum_entry_count,
                });
            }
            let entry = std::str::from_utf8(entry)
                .map_err(|_| CommittedReadError::InvalidHistorySelector)?;
            let (header, path) = entry
                .split_once('\t')
                .ok_or(CommittedReadError::InvalidHistorySelector)?;
            if !path.ends_with(".md") {
                continue;
            }
            let mut fields = header.split(' ');
            if fields.next() != Some("100644") || fields.next() != Some("blob") {
                return Err(CommittedReadError::InvalidHistorySelector);
            }
            let oid = fields
                .next()
                .ok_or(CommittedReadError::InvalidHistorySelector)?;
            let remaining = self
                .policy
                .maximum_total_markdown_bytes
                .saturating_sub(self.bytes_read);
            let limit = self.policy.maximum_markdown_bytes.min(remaining);
            let size = self.git(["cat-file", "-s", oid], 32)?;
            let size = std::str::from_utf8(&size)
                .ok()
                .and_then(|s| s.trim().parse::<u64>().ok())
                .ok_or(CommittedReadError::InvalidHistorySelector)?;
            if size > limit {
                return Err(CommittedReadError::SearchMarkdownByteLimitExceeded {
                    maximum: if remaining < self.policy.maximum_markdown_bytes {
                        self.policy.maximum_total_markdown_bytes
                    } else {
                        self.policy.maximum_markdown_bytes
                    },
                });
            }
            let bytes = self.git(
                ["cat-file", "blob", oid],
                usize::try_from(limit).unwrap_or(usize::MAX),
            )?;
            self.bytes_read = self.bytes_read.saturating_add(bytes.len() as u64);
            let record = DocumentRecord::from_markdown(PathBuf::from(path), &bytes, self.policy)
                .map_err(CommittedReadError::content)?;
            if record.metadata().document_id == id {
                if let Some(previous) = found {
                    return Err(CommittedReadError::content(
                        ContentIndexError::DuplicateDocumentId {
                            document_id: id,
                            first_path: previous.record.relative_path().to_path_buf(),
                            second_path: PathBuf::from(path),
                        },
                    ));
                }
                let markdown = String::from_utf8(bytes)
                    .map_err(|_| CommittedReadError::InvalidMarkdownEncoding { document_id: id })?;
                found = Some(HistoricalDocument { record, markdown });
            }
        }
        Ok(found)
    }

    fn git<const N: usize>(
        &self,
        arguments: [&str; N],
        limit: usize,
    ) -> Result<Vec<u8>, CommittedReadError> {
        check_operation_deadline(self.policy.scan_deadline)?;
        let result = run_git_for_read_with_output_limit(
            None,
            Some(&self.git_directory),
            arguments,
            self.policy.scan_deadline,
            limit,
        );
        check_operation_deadline(self.policy.scan_deadline)?;
        result
            .map(|output| output.stdout)
            .map_err(CommittedReadError::repository)
    }
}

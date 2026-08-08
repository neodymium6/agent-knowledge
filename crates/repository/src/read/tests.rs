use std::ffi::OsStr;
use std::fs::{self, File, TryLockError};
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

use agent_knowledge_core::{DocumentId, ProjectId};
use agent_knowledge_queue::PackagePolicy;

use super::{
    CommittedReadError, CommittedStore, LinearSearch, ReadFilter, SearchBackend,
    SearchMetadataFields, SearchPolicy,
};
use crate::{ContentPolicy, TantivySearchError, TantivySearchIndex, TantivySearchPolicy};

const LOG_ID: &str = "01K00000000000000000000001";
const RUNBOOK_ID: &str = "01K00000000000000000000002";
const SESSION_ID: &str = "01K00000000000000000000003";
const LOG_REQUEST_ID: &str = "01K00000000000000000000004";
const RUNBOOK_REQUEST_ID: &str = "01K00000000000000000000005";

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn create() -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "agent-knowledge-committed-read-test-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path)
            .unwrap_or_else(|error| panic!("read test directory must be created: {error}"));
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.0)
            && error.kind() != io::ErrorKind::NotFound
        {
            panic!("read test directory must be removed: {error}");
        }
    }
}

struct Fixture {
    _root: TestDirectory,
    repository: PathBuf,
    content: PathBuf,
}

impl Fixture {
    fn create() -> Self {
        let root = TestDirectory::create();
        let repository = root.path().join("repository");
        let seed = root.path().join("seed");
        let content = root.path().join("content");
        run_git(None, ["init", "--bare", path_text(&repository)]);
        run_git(None, ["init", "--initial-branch=main", path_text(&seed)]);
        run_git(Some(&seed), ["config", "user.name", "Fictional Writer"]);
        run_git(
            Some(&seed),
            ["config", "user.email", "writer@fictional.invalid"],
        );

        let log_path = seed
            .join("projects/fictional-project/logs/2026/07/31")
            .join(format!("035000-{LOG_ID}/index.md"));
        let runbook_path = seed
            .join("projects/fictional-project/runbooks")
            .join(format!("2026-07-31-{RUNBOOK_ID}/index.md"));
        fs::create_dir_all(
            log_path
                .parent()
                .unwrap_or_else(|| panic!("log fixture must have a parent")),
        )
        .unwrap_or_else(|error| panic!("log fixture parent must be created: {error}"));
        fs::create_dir_all(
            runbook_path
                .parent()
                .unwrap_or_else(|| panic!("runbook fixture must have a parent")),
        )
        .unwrap_or_else(|error| panic!("runbook fixture parent must be created: {error}"));
        fs::write(
            &log_path,
            format!(
                "---\nschema_version: 1\ndocument_id: {LOG_ID}\ntitle: Fictional GPU investigation\ncreated: 2026-07-31T03:50:00Z\nnode: fictional-node-a\nagent: codex\nsession: {SESSION_ID}\nrequest_id: {LOG_REQUEST_ID}\ntags:\n  - cuda\n  - performance\nstatus: active\n---\nNeedle OOM analysis.\n"
            ),
        )
        .unwrap_or_else(|error| panic!("log fixture must be written: {error}"));
        fs::write(
            &runbook_path,
            format!(
                "---\nschema_version: 1\ndocument_id: {RUNBOOK_ID}\ntitle: Fictional restart procedure\ncreated: 2026-07-31T04:00:00Z\nupdated: 2026-07-31T05:00:00Z\nrequest_id: {RUNBOOK_REQUEST_ID}\ntags:\n  - operations\nstatus: active\n---\nRestart the fictional GPU service.\n"
            ),
        )
        .unwrap_or_else(|error| panic!("runbook fixture must be written: {error}"));
        fs::write(
            runbook_path
                .parent()
                .unwrap_or_else(|| panic!("runbook fixture must have a parent"))
                .join("procedure.json"),
            b"{\"fictional\":true}\n",
        )
        .unwrap_or_else(|error| panic!("attachment fixture must be written: {error}"));
        run_git(Some(&seed), ["add", "."]);
        run_git(
            Some(&seed),
            ["commit", "-m", "Initialize fictional knowledge"],
        );
        run_git(
            Some(&seed),
            ["remote", "add", "origin", path_text(&repository)],
        );
        run_git(Some(&seed), ["push", "origin", "main"]);
        run_git(
            None,
            [
                "--git-dir",
                path_text(&repository),
                "symbolic-ref",
                "HEAD",
                "refs/heads/main",
            ],
        );
        run_git(
            None,
            [
                "--git-dir",
                path_text(&repository),
                "worktree",
                "add",
                path_text(&content),
                "main",
            ],
        );
        Self {
            _root: root,
            repository,
            content,
        }
    }

    fn store(&self) -> CommittedStore {
        CommittedStore::open(&self.repository, &self.content, "main")
            .unwrap_or_else(|error| panic!("committed store must open: {error}"))
    }
}

#[test]
fn snapshots_one_commit_and_queries_validated_documents() {
    let fixture = Fixture::create();
    let current = fixture
        .store()
        .current_commit_until(None)
        .unwrap_or_else(|error| panic!("current commit must validate: {error}"));
    let snapshot = fixture
        .store()
        .snapshot(ContentPolicy::default(), &PackagePolicy::default())
        .unwrap_or_else(|error| panic!("committed snapshot must open: {error}"));
    assert_eq!(current, snapshot.commit());
    let project = "fictional-project"
        .parse::<ProjectId>()
        .unwrap_or_else(|error| panic!("project fixture must parse: {error}"));
    let filter = ReadFilter::new(Some(project), None, None, false);

    let listed = snapshot
        .list(&filter, 10)
        .unwrap_or_else(|error| panic!("committed list must succeed: {error}"));
    assert_eq!(listed.len(), 2);
    assert!(listed[0].relative_path() < listed[1].relative_path());
    let recent = snapshot
        .recent(&filter, 10)
        .unwrap_or_else(|error| panic!("committed recent must succeed: {error}"));
    assert_eq!(recent[0].metadata().document_id.to_string(), RUNBOOK_ID);

    let log_id = LOG_ID
        .parse::<DocumentId>()
        .unwrap_or_else(|error| panic!("document fixture must parse: {error}"));
    let document = snapshot
        .get(log_id)
        .unwrap_or_else(|error| panic!("committed document must load: {error}"));
    assert!(
        std::str::from_utf8(document.markdown())
            .is_ok_and(|markdown| markdown.contains("Needle OOM analysis"))
    );
    let runbook_id = RUNBOOK_ID
        .parse::<DocumentId>()
        .unwrap_or_else(|error| panic!("document fixture must parse: {error}"));
    let bundle = snapshot
        .bundle(runbook_id)
        .unwrap_or_else(|error| panic!("committed bundle must load: {error}"));
    assert_eq!(bundle.record().metadata().document_id, runbook_id);
    assert_eq!(bundle.entries().len(), 2);
    assert_eq!(bundle.entries()[0].name(), Path::new("index.md"));
    assert_eq!(bundle.entries()[1].name(), Path::new("procedure.json"));
    assert_eq!(bundle.entries()[1].bytes(), b"{\"fictional\":true}\n");

    let search = LinearSearch::default();
    let matches = search
        .search(&snapshot, "needle", &filter, SearchPolicy::new(64, 10))
        .unwrap_or_else(|error| panic!("body search must succeed: {error}"));
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].metadata().document_id, log_id);
    let metadata_matches = search
        .search(
            &snapshot,
            "fictional-node-a",
            &filter,
            SearchPolicy::new(64, 10),
        )
        .unwrap_or_else(|error| panic!("metadata search must succeed: {error}"));
    assert_eq!(metadata_matches.len(), 1);
    assert!(!snapshot.commit().is_empty());
}

#[test]
fn filters_tags_and_disables_unselected_search_metadata() {
    let fixture = Fixture::create();
    let snapshot = fixture
        .store()
        .snapshot(ContentPolicy::default(), &PackagePolicy::default())
        .unwrap_or_else(|error| panic!("committed snapshot must open: {error}"));
    let cuda = ReadFilter::new(None, Some("cuda".into()), None, false);
    assert_eq!(
        snapshot
            .list(&cuda, 10)
            .unwrap_or_else(|error| panic!("tag filter must succeed: {error}"))
            .len(),
        1
    );
    let search = LinearSearch::new(SearchMetadataFields::new(false, false, false, false));
    assert!(
        search
            .search(
                &snapshot,
                "fictional-node-a",
                &ReadFilter::default(),
                SearchPolicy::new(64, 10),
            )
            .unwrap_or_else(|error| panic!("restricted metadata search must succeed: {error}"))
            .is_empty()
    );
}

#[test]
fn tantivy_searches_terms_phrases_metadata_and_exact_filters() {
    let fixture = Fixture::create();
    let snapshot = fixture
        .store()
        .snapshot(ContentPolicy::default(), &PackagePolicy::default())
        .unwrap_or_else(|error| panic!("committed snapshot must open: {error}"));
    let index = TantivySearchIndex::build_in_memory(&snapshot, SearchMetadataFields::default())
        .unwrap_or_else(|error| panic!("Tantivy index must build: {error}"));
    assert_eq!(index.commit(), snapshot.commit());

    let terms = index
        .search(
            &snapshot,
            "fictional OOM",
            &ReadFilter::default(),
            TantivySearchPolicy::new(64, 10),
        )
        .unwrap_or_else(|error| panic!("conjunctive search must succeed: {error}"));
    assert_eq!(terms.len(), 1);
    assert_eq!(terms[0].metadata().document_id.to_string(), LOG_ID);

    let phrase = index
        .search(
            &snapshot,
            "\"restart procedure\"",
            &ReadFilter::default(),
            TantivySearchPolicy::new(64, 10),
        )
        .unwrap_or_else(|error| panic!("phrase search must succeed: {error}"));
    assert_eq!(phrase.len(), 1);
    assert_eq!(phrase[0].metadata().document_id.to_string(), RUNBOOK_ID);

    let ranked = index
        .search(
            &snapshot,
            "GPU",
            &ReadFilter::default(),
            TantivySearchPolicy::new(64, 10),
        )
        .unwrap_or_else(|error| panic!("ranked search must succeed: {error}"));
    assert_eq!(ranked.len(), 2);
    assert_eq!(ranked[0].metadata().document_id.to_string(), LOG_ID);

    let metadata = index
        .search(
            &snapshot,
            "fictional-node-a",
            &ReadFilter::default(),
            TantivySearchPolicy::new(64, 10),
        )
        .unwrap_or_else(|error| panic!("metadata search must succeed: {error}"));
    assert_eq!(metadata.len(), 1);
    assert_eq!(metadata[0].metadata().document_id.to_string(), LOG_ID);

    let operations_only = ReadFilter::new(None, Some("operations".into()), None, false);
    let filtered = index
        .search(
            &snapshot,
            "fictional",
            &operations_only,
            TantivySearchPolicy::new(64, 10),
        )
        .unwrap_or_else(|error| panic!("filtered search must succeed: {error}"));
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].metadata().document_id.to_string(), RUNBOOK_ID);
}

#[test]
fn tantivy_honors_metadata_selection_and_query_bounds() {
    let fixture = Fixture::create();
    let snapshot = fixture
        .store()
        .snapshot(ContentPolicy::default(), &PackagePolicy::default())
        .unwrap_or_else(|error| panic!("committed snapshot must open: {error}"));
    let index = TantivySearchIndex::build_in_memory(
        &snapshot,
        SearchMetadataFields::new(false, false, false, false),
    )
    .unwrap_or_else(|error| panic!("restricted Tantivy index must build: {error}"));

    let metadata = index
        .search(
            &snapshot,
            "fictional-node-a",
            &ReadFilter::default(),
            TantivySearchPolicy::new(64, 10),
        )
        .unwrap_or_else(|error| panic!("restricted search must succeed: {error}"));
    assert!(metadata.is_empty());
    assert!(matches!(
        index.search(
            &snapshot,
            " ",
            &ReadFilter::default(),
            TantivySearchPolicy::new(64, 10),
        ),
        Err(TantivySearchError::EmptyQuery)
    ));
    assert!(matches!(
        index.search(
            &snapshot,
            "oversized",
            &ReadFilter::default(),
            TantivySearchPolicy::new(3, 10),
        ),
        Err(TantivySearchError::QueryTooLong {
            maximum: 3,
            actual: 9
        })
    ));
    assert!(matches!(
        index.search(
            &snapshot,
            "fictional",
            &ReadFilter::default(),
            TantivySearchPolicy::new(64, 0),
        ),
        Err(TantivySearchError::InvalidResultLimit)
    ));
    assert!(matches!(
        index.search(
            &snapshot,
            &format!("exact_session:{SESSION_ID}"),
            &ReadFilter::default(),
            TantivySearchPolicy::new(64, 10),
        ),
        Err(TantivySearchError::Query(_))
    ));

    let unbounded = index
        .search(
            &snapshot,
            "fictional",
            &ReadFilter::default(),
            TantivySearchPolicy::new(64, usize::MAX),
        )
        .unwrap_or_else(|error| panic!("result limit must be clamped: {error}"));
    assert_eq!(unbounded.len(), 2);
}

#[test]
fn tantivy_reopens_a_committed_disk_index_with_its_search_policy() {
    let fixture = Fixture::create();
    let snapshot = fixture
        .store()
        .snapshot(ContentPolicy::default(), &PackagePolicy::default())
        .unwrap_or_else(|error| panic!("committed snapshot must open: {error}"));
    let directory = fixture._root.path().join("search-index");
    let built = TantivySearchIndex::build_in_directory(
        &snapshot,
        SearchMetadataFields::new(false, false, false, false),
        &directory,
    )
    .unwrap_or_else(|error| panic!("disk index must build: {error}"));
    assert_eq!(built.commit(), snapshot.commit());
    drop(built);

    let reopened = TantivySearchIndex::open_directory(&directory)
        .unwrap_or_else(|error| panic!("disk index must reopen: {error}"));
    assert_eq!(reopened.commit(), snapshot.commit());
    assert!(
        reopened
            .search(
                &snapshot,
                "fictional-node-a",
                &ReadFilter::default(),
                TantivySearchPolicy::new(64, 10),
            )
            .unwrap_or_else(|error| panic!("restricted search must succeed: {error}"))
            .is_empty()
    );
    assert!(matches!(
        reopened.search(
            &snapshot,
            &format!("exact_session:{SESSION_ID}"),
            &ReadFilter::default(),
            TantivySearchPolicy::new(64, 10),
        ),
        Err(TantivySearchError::Query(_))
    ));
    let session = SESSION_ID
        .parse()
        .unwrap_or_else(|error| panic!("session fixture must parse: {error}"));
    let filtered = reopened
        .search(
            &snapshot,
            "fictional",
            &ReadFilter::new(None, None, Some(session), false),
            TantivySearchPolicy::new(64, 10),
        )
        .unwrap_or_else(|error| panic!("exact session filter must succeed: {error}"));
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].metadata().document_id.to_string(), LOG_ID);
}

#[test]
fn tantivy_disk_index_rejects_overwrite_and_manifest_mismatch() {
    let fixture = Fixture::create();
    let snapshot = fixture
        .store()
        .snapshot(ContentPolicy::default(), &PackagePolicy::default())
        .unwrap_or_else(|error| panic!("committed snapshot must open: {error}"));
    let existing = fixture._root.path().join("existing-index");
    fs::create_dir(&existing)
        .unwrap_or_else(|error| panic!("existing index directory must be created: {error}"));
    assert!(matches!(
        TantivySearchIndex::build_in_directory(
            &snapshot,
            SearchMetadataFields::default(),
            &existing,
        ),
        Err(TantivySearchError::Io(error)) if error.kind() == io::ErrorKind::AlreadyExists
    ));

    let directory = fixture._root.path().join("search-index");
    let built = TantivySearchIndex::build_in_directory(
        &snapshot,
        SearchMetadataFields::default(),
        &directory,
    )
    .unwrap_or_else(|error| panic!("disk index must build: {error}"));
    drop(built);
    let manifest_path = directory.join(".agent-knowledge-search-index.json");
    let mut manifest: serde_json::Value = serde_json::from_slice(
        &fs::read(&manifest_path)
            .unwrap_or_else(|error| panic!("disk manifest must be read: {error}")),
    )
    .unwrap_or_else(|error| panic!("disk manifest must decode: {error}"));
    manifest["commit"] = serde_json::json!("0000000000000000000000000000000000000000");
    fs::write(
        &manifest_path,
        serde_json::to_vec(&manifest)
            .unwrap_or_else(|error| panic!("changed disk commit must encode: {error}")),
    )
    .unwrap_or_else(|error| panic!("changed disk commit must be written: {error}"));
    assert!(matches!(
        TantivySearchIndex::open_directory(&directory),
        Err(TantivySearchError::DiskCommitMismatch)
    ));
    manifest["commit"] = serde_json::json!(snapshot.commit());
    manifest["document_count"] = serde_json::json!(100);
    fs::write(
        manifest_path,
        serde_json::to_vec(&manifest)
            .unwrap_or_else(|error| panic!("changed disk manifest must encode: {error}")),
    )
    .unwrap_or_else(|error| panic!("changed disk manifest must be written: {error}"));
    assert!(matches!(
        TantivySearchIndex::open_directory(&directory),
        Err(TantivySearchError::DiskDocumentCountMismatch {
            manifest: 100,
            index: 2
        })
    ));
    manifest["document_count"] = serde_json::json!(2);
    manifest["metadata_fields"]["node"] = serde_json::json!(false);
    fs::write(
        directory.join(".agent-knowledge-search-index.json"),
        serde_json::to_vec(&manifest)
            .unwrap_or_else(|error| panic!("changed disk schema manifest must encode: {error}")),
    )
    .unwrap_or_else(|error| panic!("changed disk schema manifest must be written: {error}"));
    assert!(matches!(
        TantivySearchIndex::open_directory(&directory),
        Err(TantivySearchError::DiskSchemaMismatch)
    ));
    manifest["format_version"] = serde_json::json!(2);
    fs::write(
        directory.join(".agent-knowledge-search-index.json"),
        serde_json::to_vec(&manifest)
            .unwrap_or_else(|error| panic!("unsupported disk manifest must encode: {error}")),
    )
    .unwrap_or_else(|error| panic!("unsupported disk manifest must be written: {error}"));
    assert!(matches!(
        TantivySearchIndex::open_directory(&directory),
        Err(TantivySearchError::InvalidDiskManifest)
    ));
}

#[cfg(unix)]
#[test]
fn tantivy_disk_open_pins_a_switched_current_symlink() {
    use std::os::unix::fs::symlink;

    let fixture = Fixture::create();
    let first_snapshot = fixture
        .store()
        .snapshot(ContentPolicy::default(), &PackagePolicy::default())
        .unwrap_or_else(|error| panic!("first committed snapshot must open: {error}"));
    let first_commit = first_snapshot.commit().to_owned();
    let first_directory = fixture._root.path().join("first-search-index");
    let first = TantivySearchIndex::build_in_directory(
        &first_snapshot,
        SearchMetadataFields::default(),
        &first_directory,
    )
    .unwrap_or_else(|error| panic!("first disk index must build: {error}"));
    drop(first);
    drop(first_snapshot);

    let log_path = fixture
        .content
        .join("projects/fictional-project/logs/2026/07/31")
        .join(format!("035000-{LOG_ID}/index.md"));
    let changed = fs::read_to_string(&log_path)
        .unwrap_or_else(|error| panic!("first committed Markdown must be read: {error}"))
        .replace("Needle OOM analysis.", "Needle OOM analysis changed.");
    fs::write(&log_path, changed)
        .unwrap_or_else(|error| panic!("second committed Markdown must be written: {error}"));
    run_git(Some(&fixture.content), ["add", "."]);
    run_git(
        Some(&fixture.content),
        ["commit", "-m", "Change fictional knowledge"],
    );
    let second_snapshot = fixture
        .store()
        .snapshot(ContentPolicy::default(), &PackagePolicy::default())
        .unwrap_or_else(|error| panic!("second committed snapshot must open: {error}"));
    assert_ne!(second_snapshot.commit(), first_commit);
    let second_directory = fixture._root.path().join("second-search-index");
    let second = TantivySearchIndex::build_in_directory(
        &second_snapshot,
        SearchMetadataFields::default(),
        &second_directory,
    )
    .unwrap_or_else(|error| panic!("second disk index must build: {error}"));
    drop(second);

    let current = fixture._root.path().join("current-search-index");
    let next = fixture._root.path().join("next-search-index");
    symlink(&first_directory, &current)
        .unwrap_or_else(|error| panic!("first current symlink must be created: {error}"));
    symlink(&second_directory, &next)
        .unwrap_or_else(|error| panic!("next current symlink must be created: {error}"));
    let reopened = TantivySearchIndex::open_directory_after_resolve(&current, || {
        fs::rename(&next, &current)
            .unwrap_or_else(|error| panic!("current symlink must switch atomically: {error}"));
    })
    .unwrap_or_else(|error| panic!("resolved disk index must reopen: {error}"));
    assert_eq!(reopened.commit(), first_commit);
    assert_eq!(
        fs::read_link(&current)
            .unwrap_or_else(|error| panic!("switched current symlink must be read: {error}")),
        second_directory
    );
}

#[test]
fn rejects_writer_contention_dirty_content_and_invalid_bounds() {
    let fixture = Fixture::create();
    let store = fixture.store();
    let lock = File::open(&fixture.content)
        .unwrap_or_else(|error| panic!("content lock fixture must open: {error}"));
    lock.try_lock()
        .unwrap_or_else(|error| panic!("writer fixture must acquire lock: {error}"));
    assert!(matches!(
        store.snapshot(ContentPolicy::default(), &PackagePolicy::default()),
        Err(CommittedReadError::Busy)
    ));
    lock.unlock()
        .unwrap_or_else(|error| panic!("writer fixture must release lock: {error}"));
    assert!(matches!(
        store.snapshot(
            ContentPolicy {
                scan_deadline: Some(std::time::Instant::now()),
                ..ContentPolicy::default()
            },
            &PackagePolicy::default(),
        ),
        Err(CommittedReadError::OperationDeadlineExceeded)
    ));

    let snapshot = store
        .snapshot(ContentPolicy::default(), &PackagePolicy::default())
        .unwrap_or_else(|error| panic!("snapshot after contention must open: {error}"));
    assert!(matches!(
        snapshot.list(&ReadFilter::default(), 0),
        Err(CommittedReadError::InvalidResultLimit)
    ));
    assert!(matches!(
        LinearSearch::default().search(
            &snapshot,
            " ",
            &ReadFilter::default(),
            SearchPolicy::new(64, 10),
        ),
        Err(CommittedReadError::EmptyQuery)
    ));
    assert!(matches!(
        LinearSearch::default().search(
            &snapshot,
            "not-present",
            &ReadFilter::default(),
            SearchPolicy {
                maximum_scanned_documents: 1,
                ..SearchPolicy::new(64, 10)
            },
        ),
        Err(CommittedReadError::SearchDocumentLimitExceeded { maximum: 1 })
    ));
    assert!(matches!(
        LinearSearch::default().search(
            &snapshot,
            "not-present",
            &ReadFilter::default(),
            SearchPolicy {
                maximum_scanned_markdown_bytes: 1,
                ..SearchPolicy::new(64, 10)
            },
        ),
        Err(CommittedReadError::SearchMarkdownByteLimitExceeded { maximum: 1 })
    ));
    drop(snapshot);

    fs::write(fixture.content.join("fictional-untracked.md"), "fictional")
        .unwrap_or_else(|error| panic!("dirty fixture must be written: {error}"));
    assert!(matches!(
        store.snapshot(ContentPolicy::default(), &PackagePolicy::default()),
        Err(CommittedReadError::Repository(source))
            if matches!(*source, crate::GitTransactionError::CanonicalWorktreeDirty)
    ));
}

fn path_text(path: &Path) -> &str {
    path.to_str()
        .unwrap_or_else(|| panic!("fixture path must be UTF-8"))
}

fn run_git<const N: usize>(working_directory: Option<&Path>, arguments: [&str; N]) {
    let mut command = Command::new("git");
    if let Some(directory) = working_directory {
        command.current_dir(directory);
    }
    command.args(arguments.iter().map(OsStr::new));
    let output = command
        .output()
        .unwrap_or_else(|error| panic!("Git fixture must run: {error}"));
    if !output.status.success() {
        panic!(
            "Git fixture failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn shared_snapshot_lock_blocks_publication_until_drop() {
    let fixture = Fixture::create();
    let snapshot = fixture
        .store()
        .snapshot(ContentPolicy::default(), &PackagePolicy::default())
        .unwrap_or_else(|error| panic!("committed snapshot must open: {error}"));
    let writer = File::open(&fixture.content)
        .unwrap_or_else(|error| panic!("publication lock fixture must open: {error}"));
    assert!(matches!(writer.try_lock(), Err(TryLockError::WouldBlock)));
    drop(snapshot);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    loop {
        match writer.try_lock() {
            Ok(()) => break,
            Err(TryLockError::WouldBlock) if std::time::Instant::now() < deadline => {
                std::thread::yield_now();
            }
            Err(error) => panic!("writer lock must succeed after snapshot drop: {error}"),
        }
    }
}

#[cfg(unix)]
#[test]
fn bundle_export_rejects_an_attachment_replaced_by_a_hard_link() {
    let fixture = Fixture::create();
    let snapshot = fixture
        .store()
        .snapshot(ContentPolicy::default(), &PackagePolicy::default())
        .unwrap_or_else(|error| panic!("committed snapshot must open: {error}"));
    let attachment = fixture
        .content
        .join("projects/fictional-project/runbooks")
        .join(format!("2026-07-31-{RUNBOOK_ID}/procedure.json"));
    let external = fixture._root.path().join("external.json");
    fs::write(&external, b"{\"fictional\":true}\n")
        .unwrap_or_else(|error| panic!("external fixture must be written: {error}"));
    fs::remove_file(&attachment)
        .unwrap_or_else(|error| panic!("attachment fixture must be removed: {error}"));
    fs::hard_link(&external, &attachment)
        .unwrap_or_else(|error| panic!("hard-link fixture must be created: {error}"));
    let document_id = RUNBOOK_ID
        .parse()
        .unwrap_or_else(|error| panic!("document fixture must parse: {error}"));
    assert!(matches!(
        snapshot.bundle(document_id),
        Err(CommittedReadError::Io(_))
    ));
}

#[test]
fn bundle_export_rejects_same_length_uncommitted_attachment_bytes() {
    let fixture = Fixture::create();
    let snapshot = fixture
        .store()
        .snapshot(ContentPolicy::default(), &PackagePolicy::default())
        .unwrap_or_else(|error| panic!("committed snapshot must open: {error}"));
    let attachment = fixture
        .content
        .join("projects/fictional-project/runbooks")
        .join(format!("2026-07-31-{RUNBOOK_ID}/procedure.json"));
    fs::write(&attachment, b"{\"fictional\":null}\n")
        .unwrap_or_else(|error| panic!("attachment mutation must be written: {error}"));
    let document_id = RUNBOOK_ID
        .parse()
        .unwrap_or_else(|error| panic!("document fixture must parse: {error}"));
    match snapshot.bundle(document_id) {
        Err(CommittedReadError::ContentChanged { document_id: found }) if found == document_id => {}
        other => panic!("same-length mutation must be rejected as changed content: {other:?}"),
    }
}

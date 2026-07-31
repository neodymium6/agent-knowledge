use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};

use agent_knowledge_core::BatchId;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use ulid::Ulid;

#[cfg(unix)]
use super::unix_mode_is_directory;
use super::{
    BINDING_FILE, BuildDirectory, BuiltDirectory, CLEANUP_MARKER_FILE, LEGACY_BINDING_FILE,
    MANIFEST_FILE, MANIFEST_SCHEMA_VERSION, MAXIMUM_CLEANUP_ACTIONS,
    MAXIMUM_CLEANUP_DESCRIPTOR_DEPTH, MAXIMUM_RELEASE_TREE_DEPTH, ReleaseError, ReleaseManifest,
    ReleasePolicy, ReleaseStore, cleanup_name, derived_reference_is_repairable,
    ensure_cleanup_marker, ensure_manifest, read_manifest, release_id, validate_release_tree,
    validate_release_tree_at,
};

const FIRST_BATCH: &str = "01K00000000000000000000001";
const SECOND_BATCH: &str = "01K00000000000000000000002";
const FIRST_COMMIT: &str = "1111111111111111111111111111111111111111";
const SECOND_COMMIT: &str = "2222222222222222222222222222222222222222";

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "agent-knowledge-release-store-test-{}",
            Ulid::generate()
        ));
        fs::create_dir(&path)
            .unwrap_or_else(|error| panic!("test directory must be created: {error}"));
        Self(path)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.0)
            && error.kind() != io::ErrorKind::NotFound
        {
            panic!("test directory must be removed: {error}");
        }
    }
}

fn batch(value: &str) -> BatchId {
    value
        .parse()
        .unwrap_or_else(|error| panic!("fixture batch ID must parse: {error}"))
}

fn timestamp(value: &str) -> OffsetDateTime {
    OffsetDateTime::parse(value, &Rfc3339)
        .unwrap_or_else(|error| panic!("fixture timestamp must parse: {error}"))
}

fn build(store: &ReleaseStore, batch_id: BatchId, body: &str) -> BuiltDirectory {
    let output = store
        .begin_build(batch_id)
        .unwrap_or_else(|error| panic!("release build must begin: {error}"));
    fs::write(output.path().join("index.html"), body)
        .unwrap_or_else(|error| panic!("release output must be written: {error}"));
    built(output)
}

fn built(output: BuildDirectory) -> BuiltDirectory {
    BuiltDirectory::new(output)
}

#[test]
fn prepares_immutable_releases_and_atomically_changes_current() {
    let root = TestDirectory::new();
    let releases = root.0.join("releases");
    let store = ReleaseStore::open(&releases, ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    let first_build = build(
        &store,
        batch(FIRST_BATCH),
        "<p>first fictional release</p>\n",
    );
    let first = store
        .prepare(first_build, FIRST_COMMIT, timestamp("2026-07-31T04:00:00Z"))
        .unwrap_or_else(|error| panic!("first release must prepare: {error}"));
    let active = store
        .activate(&first)
        .unwrap_or_else(|error| panic!("first release must activate: {error}"));
    assert_eq!(active.commit(), FIRST_COMMIT);
    assert_eq!(
        fs::read_link(releases.join("current"))
            .unwrap_or_else(|error| panic!("current link must be readable: {error}")),
        PathBuf::from("by-id").join(first.release_id())
    );

    let second_build = build(
        &store,
        batch(SECOND_BATCH),
        "<p>second fictional release</p>\n",
    );
    let second = store
        .prepare(
            second_build,
            SECOND_COMMIT,
            timestamp("2026-07-31T04:05:00Z"),
        )
        .unwrap_or_else(|error| panic!("second release must prepare: {error}"));
    assert_eq!(
        store
            .active_release()
            .unwrap_or_else(|error| panic!("active release must validate: {error}"))
            .unwrap_or_else(|| panic!("first release must remain active"))
            .commit(),
        FIRST_COMMIT
    );
    store
        .activate(&second)
        .unwrap_or_else(|error| panic!("second release must activate: {error}"));
    assert_eq!(
        store
            .active_release()
            .unwrap_or_else(|error| panic!("active release must validate: {error}"))
            .unwrap_or_else(|| panic!("second release must be active"))
            .commit(),
        SECOND_COMMIT
    );
    assert!(releases.join("by-id").join(first.release_id()).is_dir());
    assert!(releases.join("by-id").join(second.release_id()).is_dir());
}

#[test]
fn activation_is_idempotent_for_the_same_release() {
    let root = TestDirectory::new();
    let store = ReleaseStore::open(root.0.join("releases"), ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    let build = build(&store, batch(FIRST_BATCH), "fictional output\n");
    let release = store
        .prepare(build, FIRST_COMMIT, timestamp("2026-07-31T04:00:00Z"))
        .unwrap_or_else(|error| panic!("release must prepare: {error}"));
    let first = store
        .activate(&release)
        .unwrap_or_else(|error| panic!("release must activate: {error}"));
    let second = store
        .activate(&release)
        .unwrap_or_else(|error| panic!("release reactivation must succeed: {error}"));
    assert_eq!(first, second);
}

#[test]
fn preparation_recovers_after_the_release_rename() {
    let root = TestDirectory::new();
    let store = ReleaseStore::open(root.0.join("releases"), ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    let build = build(&store, batch(FIRST_BATCH), "fictional output\n");
    let created_at = timestamp("2026-07-31T04:00:00Z");
    let first = store
        .prepare(build, FIRST_COMMIT, created_at)
        .unwrap_or_else(|error| panic!("release must prepare: {error}"));
    let recovered = store
        .resume_prepare(batch(FIRST_BATCH), FIRST_COMMIT)
        .unwrap_or_else(|error| panic!("renamed release must recover: {error}"));

    assert_eq!(first, recovered);
}

#[test]
fn preparation_removes_a_recovered_duplicate_staging_tree() {
    let root = TestDirectory::new();
    let releases = root.0.join("releases");
    let store = ReleaseStore::open(&releases, ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    let build = build(&store, batch(FIRST_BATCH), "fictional output\n");
    let created_at = timestamp("2026-07-31T04:00:00Z");
    let release = store
        .prepare(build, FIRST_COMMIT, created_at)
        .unwrap_or_else(|error| panic!("release must prepare: {error}"));
    let prepared = releases.join("by-id").join(release.release_id());
    let recovered_staging = releases.join(".staging").join(FIRST_BATCH);
    let recovered_site = recovered_staging.join("site");
    fs::create_dir_all(&recovered_site)
        .unwrap_or_else(|error| panic!("recovered staging directory must be created: {error}"));
    for name in ["index.html", ".agent-knowledge-release.json"] {
        fs::copy(prepared.join(name), recovered_site.join(name))
            .unwrap_or_else(|error| panic!("recovered staging file must be copied: {error}"));
    }

    store
        .resume_prepare(batch(FIRST_BATCH), FIRST_COMMIT)
        .unwrap_or_else(|error| panic!("duplicated staging state must recover: {error}"));
    assert!(!recovered_staging.exists());
}

#[test]
fn recovers_a_prepared_release_after_reopening_the_store() {
    let root = TestDirectory::new();
    let releases = root.0.join("releases");
    let prepared = {
        let store = ReleaseStore::open(&releases, ReleasePolicy::default())
            .unwrap_or_else(|error| panic!("release store must open: {error}"));
        let build = build(&store, batch(FIRST_BATCH), "fictional output\n");
        store
            .prepare(build, FIRST_COMMIT, timestamp("2026-07-31T04:00:00Z"))
            .unwrap_or_else(|error| panic!("release must prepare: {error}"))
    };
    let reopened = ReleaseStore::open(&releases, ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("release store must reopen: {error}"));

    let recovered = reopened
        .prepared_for_commit(FIRST_COMMIT)
        .unwrap_or_else(|error| panic!("prepared release lookup must succeed: {error}"))
        .unwrap_or_else(|| panic!("prepared release must be recovered"));
    assert_eq!(recovered, prepared);
    assert_eq!(
        reopened
            .activate(&recovered)
            .unwrap_or_else(|error| panic!("recovered release must activate: {error}"))
            .commit(),
        FIRST_COMMIT
    );
}

#[test]
fn resumes_from_a_durable_batch_intent_before_promotion() {
    let root = TestDirectory::new();
    let releases = root.0.join("releases");
    let store = ReleaseStore::open(&releases, ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    let output = build(&store, batch(FIRST_BATCH), "fictional output\n");
    let created_at = timestamp("2026-07-31T04:00:00Z");
    let content_revision = validate_release_tree_at(
        output.path(),
        &output.0.batch_handle,
        ReleasePolicy::default(),
        false,
    )
    .unwrap_or_else(|error| panic!("staged output must validate: {error}"));
    let manifest = ReleaseManifest {
        schema_version: MANIFEST_SCHEMA_VERSION,
        release_id: release_id(created_at, FIRST_COMMIT),
        commit: FIRST_COMMIT.into(),
        content_revision,
        created_at,
    };
    ensure_manifest(&output.path().join(MANIFEST_FILE), &manifest)
        .unwrap_or_else(|error| panic!("staged manifest must be durable: {error}"));
    store
        .ensure_batch_intent(batch(FIRST_BATCH), &manifest)
        .unwrap_or_else(|error| panic!("batch intent must be durable: {error}"));
    drop(output);
    assert!(matches!(
        store.discard_build(batch(FIRST_BATCH)),
        Err(ReleaseError::BuildRecoveryRequired)
    ));
    drop(store);

    let reopened = ReleaseStore::open(&releases, ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("release store must reopen: {error}"));
    let recovered = reopened
        .resume_prepare(batch(FIRST_BATCH), FIRST_COMMIT)
        .unwrap_or_else(|error| panic!("durable batch intent must resume: {error}"));
    assert_eq!(recovered.release_id(), manifest.release_id);
    assert!(!releases.join("by-batch").join(FIRST_BATCH).exists());
    assert_eq!(
        reopened
            .prepared_for_commit(FIRST_COMMIT)
            .unwrap_or_else(|error| panic!("commit lookup must succeed: {error}"))
            .unwrap_or_else(|| panic!("resumed release must be indexed")),
        recovered
    );
}

#[test]
fn resumes_cleanup_from_a_deterministic_batch_tombstone() {
    let root = TestDirectory::new();
    let releases = root.0.join("releases");
    let store = ReleaseStore::open(&releases, ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    let prepared = store
        .prepare(
            build(&store, batch(FIRST_BATCH), "fictional output\n"),
            FIRST_COMMIT,
            timestamp("2026-07-31T04:00:00Z"),
        )
        .unwrap_or_else(|error| panic!("release must prepare: {error}"));
    let recovered_batch = releases.join(".staging").join(FIRST_BATCH);
    let recovered_site = recovered_batch.join("site");
    fs::create_dir_all(&recovered_site)
        .unwrap_or_else(|error| panic!("recovered staging directory must be created: {error}"));
    for name in ["index.html", MANIFEST_FILE] {
        fs::copy(
            releases
                .join("by-id")
                .join(prepared.release_id())
                .join(name),
            recovered_site.join(name),
        )
        .unwrap_or_else(|error| panic!("recovered staged file must be copied: {error}"));
    }
    ensure_cleanup_marker(&recovered_batch, batch(FIRST_BATCH))
        .unwrap_or_else(|error| panic!("cleanup marker must be durable: {error}"));
    let recovered_handle = fs::File::open(&recovered_batch)
        .unwrap_or_else(|error| panic!("recovered batch must be opened: {error}"));
    store
        .ensure_cleanup_intent(batch(FIRST_BATCH), &recovered_handle)
        .unwrap_or_else(|error| panic!("cleanup intent must be durable: {error}"));
    let tombstone = releases
        .join(".staging")
        .join(cleanup_name(batch(FIRST_BATCH)));
    fs::rename(&recovered_batch, &tombstone)
        .unwrap_or_else(|error| panic!("cleanup rename must be simulated: {error}"));
    let manifest = read_manifest(&releases.join("by-id").join(prepared.release_id()))
        .unwrap_or_else(|error| panic!("prepared manifest must be readable: {error}"));
    store
        .ensure_batch_intent(batch(FIRST_BATCH), &manifest)
        .unwrap_or_else(|error| panic!("batch intent must be durable: {error}"));

    let recovered = store
        .resume_prepare(batch(FIRST_BATCH), FIRST_COMMIT)
        .unwrap_or_else(|error| panic!("cleanup tombstone must recover: {error}"));
    assert_eq!(recovered, prepared);
    assert!(!tombstone.exists());
    assert!(!releases.join("by-batch").join(FIRST_BATCH).exists());
}

#[test]
fn discard_finishes_a_deterministic_cleanup_tombstone() {
    let root = TestDirectory::new();
    let releases = root.0.join("releases");
    let store = ReleaseStore::open(&releases, ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    let output = build(&store, batch(FIRST_BATCH), "partial fictional output\n");
    drop(output);
    let staged = releases.join(".staging").join(FIRST_BATCH);
    ensure_cleanup_marker(&staged, batch(FIRST_BATCH))
        .unwrap_or_else(|error| panic!("cleanup marker must be durable: {error}"));
    let staged_handle = fs::File::open(&staged)
        .unwrap_or_else(|error| panic!("staged batch must be opened: {error}"));
    store
        .ensure_cleanup_intent(batch(FIRST_BATCH), &staged_handle)
        .unwrap_or_else(|error| panic!("cleanup intent must be durable: {error}"));
    let tombstone = releases
        .join(".staging")
        .join(cleanup_name(batch(FIRST_BATCH)));
    fs::rename(&staged, &tombstone)
        .unwrap_or_else(|error| panic!("cleanup rename must be simulated: {error}"));

    assert!(matches!(
        store.begin_build(batch(FIRST_BATCH)),
        Err(ReleaseError::BuildRecoveryRequired)
    ));
    store
        .discard_build(batch(FIRST_BATCH))
        .unwrap_or_else(|error| panic!("cleanup tombstone must be discarded: {error}"));
    assert!(!tombstone.exists());
    let rebuilt = store
        .begin_build(batch(FIRST_BATCH))
        .unwrap_or_else(|error| panic!("cleaned batch must be reusable: {error}"));
    drop(rebuilt);
}

#[test]
fn discard_repairs_an_interrupted_cleanup_marker_write() {
    let root = TestDirectory::new();
    let releases = root.0.join("releases");
    let store = ReleaseStore::open(&releases, ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    let output = build(&store, batch(FIRST_BATCH), "partial fictional output\n");
    drop(output);
    let staged = releases.join(".staging").join(FIRST_BATCH);
    let staged_handle = fs::File::open(&staged)
        .unwrap_or_else(|error| panic!("staged batch must be opened: {error}"));
    store
        .ensure_cleanup_intent(batch(FIRST_BATCH), &staged_handle)
        .unwrap_or_else(|error| panic!("cleanup intent must be durable: {error}"));
    fs::write(
        staged.join(CLEANUP_MARKER_FILE),
        "interrupted fictional marker",
    )
    .unwrap_or_else(|error| panic!("partial cleanup marker must be written: {error}"));

    store
        .discard_build(batch(FIRST_BATCH))
        .unwrap_or_else(|error| panic!("partial cleanup marker must be repaired: {error}"));
    assert!(!staged.exists());
    assert!(!releases.join("cleanup-intent").join(FIRST_BATCH).exists());
}

#[test]
fn cleanup_intent_rejects_a_replaced_private_tombstone() {
    let root = TestDirectory::new();
    let releases = root.0.join("releases");
    let store = ReleaseStore::open(&releases, ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    let output = build(&store, batch(FIRST_BATCH), "partial fictional output\n");
    drop(output);
    let staged = releases.join(".staging").join(FIRST_BATCH);
    let staged_handle = fs::File::open(&staged)
        .unwrap_or_else(|error| panic!("staged batch must be opened: {error}"));
    store
        .ensure_cleanup_intent(batch(FIRST_BATCH), &staged_handle)
        .unwrap_or_else(|error| panic!("cleanup intent must be durable: {error}"));
    ensure_cleanup_marker(&staged, batch(FIRST_BATCH))
        .unwrap_or_else(|error| panic!("cleanup marker must be durable: {error}"));
    let tombstone = releases
        .join(".staging")
        .join(cleanup_name(batch(FIRST_BATCH)));
    fs::rename(&staged, &tombstone)
        .unwrap_or_else(|error| panic!("cleanup tombstone must be created: {error}"));
    let detached = releases.join(".staging").join("detached-fictional-cleanup");
    fs::rename(&tombstone, &detached)
        .unwrap_or_else(|error| panic!("cleanup tombstone must be detached: {error}"));
    fs::create_dir(&tombstone)
        .unwrap_or_else(|error| panic!("replacement tombstone must be created: {error}"));
    fs::write(tombstone.join("preserve.txt"), "fictional replacement\n")
        .unwrap_or_else(|error| panic!("replacement fixture must be written: {error}"));

    assert!(matches!(
        store.discard_build(batch(FIRST_BATCH)),
        Err(ReleaseError::InvalidCleanupIntent)
    ));
    assert!(tombstone.join("preserve.txt").is_file());
    assert!(detached.join("site").join("index.html").is_file());
}

#[cfg(unix)]
#[test]
fn discard_does_not_follow_links_from_the_pinned_cleanup_tree() {
    use std::os::unix::fs::symlink;

    let root = TestDirectory::new();
    let releases = root.0.join("releases");
    let outside = root.0.join("fictional-outside");
    fs::create_dir(&outside)
        .unwrap_or_else(|error| panic!("outside fixture must be created: {error}"));
    fs::write(outside.join("sentinel.txt"), "preserve fictional data\n")
        .unwrap_or_else(|error| panic!("outside sentinel must be written: {error}"));
    let store = ReleaseStore::open(&releases, ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    let output = build(&store, batch(FIRST_BATCH), "partial fictional output\n");
    symlink(&outside, output.path().join("outside-link"))
        .unwrap_or_else(|error| panic!("cleanup symlink fixture must be created: {error}"));
    drop(output);

    store
        .discard_build(batch(FIRST_BATCH))
        .unwrap_or_else(|error| panic!("linked partial build must be discarded: {error}"));
    assert_eq!(
        fs::read_to_string(outside.join("sentinel.txt"))
            .unwrap_or_else(|error| panic!("outside sentinel must remain readable: {error}")),
        "preserve fictional data\n"
    );
}

#[cfg(unix)]
#[test]
fn discard_removes_unix_sockets_as_non_directory_entries() {
    use std::os::unix::net::UnixListener;

    let root = TestDirectory::new();
    let store = ReleaseStore::open(root.0.join("releases"), ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    let output = build(&store, batch(FIRST_BATCH), "partial fictional output\n");
    let socket = root
        .0
        .join("releases")
        .join(".staging")
        .join(FIRST_BATCH)
        .join("site")
        .join("fictional.sock");
    let short_socket =
        std::env::temp_dir().join(format!("agent-knowledge-socket-{}", Ulid::generate()));
    let listener = match UnixListener::bind(&short_socket) {
        Ok(listener) => listener,
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => return,
        Err(error) => panic!("Unix socket fixture must bind: {error}"),
    };
    fs::rename(&short_socket, &socket)
        .unwrap_or_else(|error| panic!("Unix socket fixture must move into staging: {error}"));
    drop(listener);
    drop(output);

    store
        .discard_build(batch(FIRST_BATCH))
        .unwrap_or_else(|error| panic!("Unix socket output must be discarded: {error}"));
    assert!(!socket.exists());
}

#[cfg(unix)]
#[test]
fn cleanup_classifies_only_directory_modes_as_directories() {
    use nix::sys::stat::SFlag;

    assert!(unix_mode_is_directory(SFlag::S_IFDIR.bits()));
    assert!(!unix_mode_is_directory(SFlag::S_IFSOCK.bits()));
    assert!(!unix_mode_is_directory(SFlag::S_IFREG.bits()));
}

#[test]
fn discard_resumes_after_a_bounded_cleanup_pass() {
    let root = TestDirectory::new();
    let store = ReleaseStore::open(root.0.join("releases"), ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    let output = store
        .begin_build(batch(FIRST_BATCH))
        .unwrap_or_else(|error| panic!("release build must begin: {error}"));
    for index in 0..=MAXIMUM_CLEANUP_ACTIONS {
        fs::write(
            output.path().join(format!("partial-{index:04}.html")),
            "fictional\n",
        )
        .unwrap_or_else(|error| panic!("partial output must be written: {error}"));
    }
    drop(output);

    assert!(matches!(
        store.discard_build(batch(FIRST_BATCH)),
        Err(ReleaseError::CleanupIncomplete)
    ));
    store
        .discard_build(batch(FIRST_BATCH))
        .unwrap_or_else(|error| panic!("bounded cleanup must resume: {error}"));
}

#[test]
fn discard_flattens_trees_deeper_than_the_descriptor_budget() {
    let root = TestDirectory::new();
    let store = ReleaseStore::open(root.0.join("releases"), ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    let output = store
        .begin_build(batch(FIRST_BATCH))
        .unwrap_or_else(|error| panic!("release build must begin: {error}"));
    let mut deepest = output.path().to_path_buf();
    for index in 0..=MAXIMUM_CLEANUP_DESCRIPTOR_DEPTH {
        deepest.push(format!("depth-{index:03}"));
        fs::create_dir(&deepest)
            .unwrap_or_else(|error| panic!("deep cleanup fixture must be created: {error}"));
    }
    fs::write(deepest.join("partial.html"), "fictional\n")
        .unwrap_or_else(|error| panic!("deep output must be written: {error}"));
    drop(output);

    loop {
        match store.discard_build(batch(FIRST_BATCH)) {
            Ok(()) => break,
            Err(ReleaseError::CleanupIncomplete) => {}
            Err(error) => panic!("deep cleanup must remain resumable: {error}"),
        }
    }
}

#[cfg(unix)]
#[test]
fn rejects_a_malformed_batch_intent_as_recovery_metadata() {
    use std::os::unix::fs::symlink;

    let root = TestDirectory::new();
    let releases = root.0.join("releases");
    let store = ReleaseStore::open(&releases, ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    symlink(
        "../../fictional-release",
        releases.join("by-batch").join(FIRST_BATCH),
    )
    .unwrap_or_else(|error| panic!("malformed batch intent fixture must be created: {error}"));

    assert!(matches!(
        store.discard_build(batch(FIRST_BATCH)),
        Err(ReleaseError::InvalidBatchIntent)
    ));
}

#[test]
fn retrying_an_older_release_does_not_regress_commit_lookup() {
    let root = TestDirectory::new();
    let releases = root.0.join("releases");
    let store = ReleaseStore::open(&releases, ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    let first_build = build(&store, batch(FIRST_BATCH), "first fictional output\n");
    let first_created_at = timestamp("2026-07-31T04:00:00Z");
    let first = store
        .prepare(first_build, FIRST_COMMIT, first_created_at)
        .unwrap_or_else(|error| panic!("first release must prepare: {error}"));
    let second_build = build(&store, batch(SECOND_BATCH), "second fictional output\n");
    let second = store
        .prepare(
            second_build,
            FIRST_COMMIT,
            timestamp("2026-07-31T04:05:00Z"),
        )
        .unwrap_or_else(|error| panic!("second release must prepare: {error}"));

    store
        .resume_prepare(batch(FIRST_BATCH), FIRST_COMMIT)
        .unwrap_or_else(|error| panic!("older release retry must succeed: {error}"));
    let recovered = store
        .prepared_for_commit(FIRST_COMMIT)
        .unwrap_or_else(|error| panic!("prepared release lookup must succeed: {error}"))
        .unwrap_or_else(|| panic!("newest release must remain indexed"));
    assert_ne!(first, second);
    assert_eq!(recovered, second);
}

#[cfg(unix)]
#[test]
fn batch_intent_recovers_the_exact_release_without_regressing_commit_lookup() {
    use std::os::unix::fs::symlink;

    let root = TestDirectory::new();
    let releases = root.0.join("releases");
    let store = ReleaseStore::open(&releases, ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    let first = store
        .prepare(
            build(&store, batch(FIRST_BATCH), "first fictional output\n"),
            FIRST_COMMIT,
            timestamp("2026-07-31T04:00:00Z"),
        )
        .unwrap_or_else(|error| panic!("first release must prepare: {error}"));
    let second = store
        .prepare(
            build(&store, batch(SECOND_BATCH), "second fictional output\n"),
            FIRST_COMMIT,
            timestamp("2026-07-31T04:05:00Z"),
        )
        .unwrap_or_else(|error| panic!("second release must prepare: {error}"));
    let recovered_batch = releases.join(".staging").join(FIRST_BATCH);
    let recovered_site = recovered_batch.join("site");
    fs::create_dir_all(&recovered_site)
        .unwrap_or_else(|error| panic!("recovered staging directory must be created: {error}"));
    for name in ["index.html", MANIFEST_FILE] {
        fs::copy(
            releases.join("by-id").join(first.release_id()).join(name),
            recovered_site.join(name),
        )
        .unwrap_or_else(|error| panic!("recovered staged file must be copied: {error}"));
    }
    symlink(
        PathBuf::from("..").join("by-id").join(first.release_id()),
        releases.join("by-batch").join(FIRST_BATCH),
    )
    .unwrap_or_else(|error| panic!("batch intent fixture must be created: {error}"));

    let recovered = store
        .resume_prepare(batch(FIRST_BATCH), FIRST_COMMIT)
        .unwrap_or_else(|error| panic!("exact batch release must recover: {error}"));
    assert_eq!(recovered, first);
    assert_eq!(
        store
            .prepared_for_commit(FIRST_COMMIT)
            .unwrap_or_else(|error| panic!("commit lookup must succeed: {error}"))
            .unwrap_or_else(|| panic!("newest release must remain indexed")),
        second
    );
    assert!(!recovered_batch.exists());
    assert!(!releases.join("by-batch").join(FIRST_BATCH).exists());
}

#[test]
fn repairs_a_corrupt_derived_commit_reference() {
    let root = TestDirectory::new();
    let releases = root.0.join("releases");
    let store = ReleaseStore::open(&releases, ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    let build = build(&store, batch(FIRST_BATCH), "fictional output\n");
    let prepared = store
        .prepare(build, FIRST_COMMIT, timestamp("2026-07-31T04:00:00Z"))
        .unwrap_or_else(|error| panic!("release must prepare: {error}"));
    let reference = releases.join("by-commit").join(FIRST_COMMIT);
    fs::remove_file(&reference)
        .unwrap_or_else(|error| panic!("derived reference must be removed: {error}"));
    fs::write(&reference, "corrupt fictional reference\n")
        .unwrap_or_else(|error| panic!("corrupt reference fixture must be written: {error}"));

    assert!(matches!(
        store.prepared_for_commit(FIRST_COMMIT),
        Err(ReleaseError::InvalidCommitReference)
    ));
    let repaired = store
        .repair_commit_reference(prepared.release_id())
        .unwrap_or_else(|error| panic!("derived reference must be repairable: {error}"));
    assert_eq!(repaired, prepared);
    assert_eq!(
        store
            .prepared_for_commit(FIRST_COMMIT)
            .unwrap_or_else(|error| panic!("repaired lookup must succeed: {error}"))
            .unwrap_or_else(|| panic!("repaired release must be found")),
        prepared
    );
}

#[test]
fn transient_io_errors_do_not_authorize_derived_reference_replacement() {
    assert!(!derived_reference_is_repairable(&ReleaseError::Io(
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            "fictional transient read failure",
        ),
    )));
    assert!(derived_reference_is_repairable(&ReleaseError::Io(
        io::Error::new(io::ErrorKind::NotFound, "fictional missing release"),
    )));
}

#[test]
fn tighter_policy_replaces_an_oversized_derived_commit_target() {
    let root = TestDirectory::new();
    let releases = root.0.join("releases");
    {
        let store = ReleaseStore::open(&releases, ReleasePolicy::default())
            .unwrap_or_else(|error| panic!("release store must open: {error}"));
        store
            .prepare(
                build(&store, batch(FIRST_BATCH), "oversized fictional output\n"),
                FIRST_COMMIT,
                timestamp("2026-07-31T04:00:00Z"),
            )
            .unwrap_or_else(|error| panic!("large release must prepare: {error}"));
    }
    let strict = ReleasePolicy {
        maximum_entries: 10,
        maximum_file_bytes: 8,
        maximum_total_bytes: 8,
    };
    let reopened = ReleaseStore::open(&releases, strict)
        .unwrap_or_else(|error| panic!("release store must reopen: {error}"));
    let replacement = reopened
        .prepare(
            build(&reopened, batch(SECOND_BATCH), "small\n"),
            FIRST_COMMIT,
            timestamp("2026-07-31T04:05:00Z"),
        )
        .unwrap_or_else(|error| panic!("small replacement must prepare: {error}"));

    assert_eq!(
        reopened
            .prepared_for_commit(FIRST_COMMIT)
            .unwrap_or_else(|error| panic!("strict commit lookup must succeed: {error}"))
            .unwrap_or_else(|| panic!("replacement release must be indexed")),
        replacement
    );
}

#[cfg(target_os = "linux")]
#[test]
fn build_directory_keeps_its_directory_lease_after_store_drop() {
    let root = TestDirectory::new();
    let output = {
        let store = ReleaseStore::open(root.0.join("releases"), ReleasePolicy::default())
            .unwrap_or_else(|error| panic!("release store must open: {error}"));
        store
            .begin_build(batch(FIRST_BATCH))
            .unwrap_or_else(|error| panic!("release build must begin: {error}"))
    };

    fs::write(output.path().join("index.html"), "fictional output\n")
        .unwrap_or_else(|error| panic!("leased build output must remain writable: {error}"));
}

#[cfg(target_os = "linux")]
#[test]
fn build_directory_keeps_its_identity_after_entry_replacement() {
    let root = TestDirectory::new();
    let releases = root.0.join("releases");
    let store = ReleaseStore::open(&releases, ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    let output = store
        .begin_build(batch(FIRST_BATCH))
        .unwrap_or_else(|error| panic!("release build must begin: {error}"));
    let configured = releases.join(".staging").join(FIRST_BATCH);
    let detached = releases.join(".staging").join("detached");
    fs::rename(&configured, &detached)
        .unwrap_or_else(|error| panic!("build directory must be moved: {error}"));
    fs::create_dir(&configured)
        .unwrap_or_else(|error| panic!("replacement build directory must be created: {error}"));

    fs::write(output.path().join("index.html"), "fictional output\n")
        .unwrap_or_else(|error| panic!("pinned output must remain writable: {error}"));
    assert!(detached.join("site").join("index.html").is_file());
    assert!(!configured.join("site").join("index.html").exists());
    assert!(matches!(
        store.prepare(
            built(output),
            FIRST_COMMIT,
            timestamp("2026-07-31T04:00:00Z"),
        ),
        Err(ReleaseError::StorageBindingMismatch)
    ));
    assert!(
        !releases
            .join("by-id")
            .join(format!("20260731T040000Z-{FIRST_COMMIT}"))
            .exists()
    );
}

#[test]
fn active_build_lease_blocks_prepare_and_discard() {
    let root = TestDirectory::new();
    let store = ReleaseStore::open(root.0.join("releases"), ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    let output = store
        .begin_build(batch(FIRST_BATCH))
        .unwrap_or_else(|error| panic!("release build must begin: {error}"));
    fs::write(output.path().join("index.html"), "fictional output\n")
        .unwrap_or_else(|error| panic!("release output must be written: {error}"));

    assert!(matches!(
        store.resume_prepare(batch(FIRST_BATCH), FIRST_COMMIT),
        Err(ReleaseError::ReleaseStoreBusy)
    ));
    assert!(matches!(
        store.discard_build(batch(FIRST_BATCH)),
        Err(ReleaseError::ReleaseStoreBusy)
    ));
    store
        .prepare(
            built(output),
            FIRST_COMMIT,
            timestamp("2026-07-31T04:00:00Z"),
        )
        .unwrap_or_else(|error| panic!("finished build must prepare: {error}"));
}

#[test]
fn discarded_build_output_can_be_rebuilt_for_the_same_batch() {
    let root = TestDirectory::new();
    let store = ReleaseStore::open(root.0.join("releases"), ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    let first = store
        .begin_build(batch(FIRST_BATCH))
        .unwrap_or_else(|error| panic!("first build must begin: {error}"));
    fs::write(
        first.path().join("partial.html"),
        "partial fictional output\n",
    )
    .unwrap_or_else(|error| panic!("partial output must be written: {error}"));
    drop(first);

    store
        .discard_build(batch(FIRST_BATCH))
        .unwrap_or_else(|error| panic!("partial build must be discarded: {error}"));
    let rebuilt = store
        .begin_build(batch(FIRST_BATCH))
        .unwrap_or_else(|error| panic!("discarded batch must be reusable: {error}"));
    assert!(
        fs::read_dir(rebuilt.path())
            .unwrap_or_else(|error| panic!("rebuilt output must be readable: {error}"))
            .next()
            .is_none()
    );
}

#[cfg(unix)]
#[test]
fn rejects_generated_symlinks_before_publication() {
    use std::os::unix::fs::symlink;

    let root = TestDirectory::new();
    let store = ReleaseStore::open(root.0.join("releases"), ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    let output = store
        .begin_build(batch(FIRST_BATCH))
        .unwrap_or_else(|error| panic!("release build must begin: {error}"));
    fs::write(output.path().join("index.html"), "fictional output\n")
        .unwrap_or_else(|error| panic!("release output must be written: {error}"));
    symlink("/fictional/private", output.path().join("escape"))
        .unwrap_or_else(|error| panic!("unsafe fixture symlink must be created: {error}"));
    assert!(matches!(
        store.prepare(
            built(output),
            FIRST_COMMIT,
            timestamp("2026-07-31T04:00:00Z"),
        ),
        Err(ReleaseError::UnsafeOutput(_))
    ));
    assert!(
        store
            .active_release()
            .unwrap_or_else(|error| panic!("empty active state must validate: {error}"))
            .is_none()
    );
}

#[cfg(unix)]
#[test]
fn invalid_new_output_keeps_the_previous_release_active() {
    use std::os::unix::fs::symlink;

    let root = TestDirectory::new();
    let store = ReleaseStore::open(root.0.join("releases"), ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    let first_build = build(&store, batch(FIRST_BATCH), "first fictional output\n");
    let first = store
        .prepare(first_build, FIRST_COMMIT, timestamp("2026-07-31T04:00:00Z"))
        .unwrap_or_else(|error| panic!("first release must prepare: {error}"));
    store
        .activate(&first)
        .unwrap_or_else(|error| panic!("first release must activate: {error}"));

    let output = store
        .begin_build(batch(SECOND_BATCH))
        .unwrap_or_else(|error| panic!("second build must begin: {error}"));
    fs::write(
        output.path().join("index.html"),
        "second fictional output\n",
    )
    .unwrap_or_else(|error| panic!("second output must be written: {error}"));
    symlink("/fictional/private", output.path().join("escape"))
        .unwrap_or_else(|error| panic!("unsafe fixture symlink must be created: {error}"));
    assert!(matches!(
        store.prepare(
            built(output),
            SECOND_COMMIT,
            timestamp("2026-07-31T04:05:00Z"),
        ),
        Err(ReleaseError::UnsafeOutput(_))
    ));
    assert_eq!(
        store
            .active_release()
            .unwrap_or_else(|error| panic!("active release must validate: {error}"))
            .unwrap_or_else(|| panic!("first release must remain active"))
            .commit(),
        FIRST_COMMIT
    );
}

#[test]
fn changed_prepared_output_cannot_replace_the_active_release() {
    let root = TestDirectory::new();
    let releases = root.0.join("releases");
    let store = ReleaseStore::open(&releases, ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    let first_build = build(&store, batch(FIRST_BATCH), "first fictional output\n");
    let first = store
        .prepare(first_build, FIRST_COMMIT, timestamp("2026-07-31T04:00:00Z"))
        .unwrap_or_else(|error| panic!("first release must prepare: {error}"));
    store
        .activate(&first)
        .unwrap_or_else(|error| panic!("first release must activate: {error}"));

    let second_build = build(&store, batch(SECOND_BATCH), "second fictional output\n");
    let second = store
        .prepare(
            second_build,
            SECOND_COMMIT,
            timestamp("2026-07-31T04:05:00Z"),
        )
        .unwrap_or_else(|error| panic!("second release must prepare: {error}"));
    fs::write(
        releases
            .join("by-id")
            .join(second.release_id())
            .join("index.html"),
        "changed fictional output\n",
    )
    .unwrap_or_else(|error| panic!("prepared output fixture must be changed: {error}"));

    assert!(matches!(
        store.activate(&second),
        Err(ReleaseError::InvalidManifest)
    ));
    assert_eq!(
        store
            .active_release()
            .unwrap_or_else(|error| panic!("active release must validate: {error}"))
            .unwrap_or_else(|| panic!("first release must remain active"))
            .commit(),
        FIRST_COMMIT
    );
}

#[cfg(unix)]
#[test]
fn hard_linked_manifest_cannot_be_activated() {
    let root = TestDirectory::new();
    let releases = root.0.join("releases");
    let store = ReleaseStore::open(&releases, ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    let build = build(&store, batch(FIRST_BATCH), "fictional output\n");
    let release = store
        .prepare(build, FIRST_COMMIT, timestamp("2026-07-31T04:00:00Z"))
        .unwrap_or_else(|error| panic!("release must prepare: {error}"));
    fs::hard_link(
        releases
            .join("by-id")
            .join(release.release_id())
            .join(".agent-knowledge-release.json"),
        root.0.join("linked-manifest.json"),
    )
    .unwrap_or_else(|error| panic!("manifest hard link fixture must be created: {error}"));

    assert!(matches!(
        store.activate(&release),
        Err(ReleaseError::UnsafeOutput(_))
    ));
}

#[test]
fn cloned_stores_serialize_activation_results() {
    let root = TestDirectory::new();
    let store = ReleaseStore::open(root.0.join("releases"), ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    let first_build = build(&store, batch(FIRST_BATCH), "first fictional output\n");
    let first = store
        .prepare(first_build, FIRST_COMMIT, timestamp("2026-07-31T04:00:00Z"))
        .unwrap_or_else(|error| panic!("first release must prepare: {error}"));
    let second_build = build(&store, batch(SECOND_BATCH), "second fictional output\n");
    let second = store
        .prepare(
            second_build,
            SECOND_COMMIT,
            timestamp("2026-07-31T04:05:00Z"),
        )
        .unwrap_or_else(|error| panic!("second release must prepare: {error}"));
    let barrier = Arc::new(Barrier::new(3));
    let first_store = store.clone();
    let first_barrier = Arc::clone(&barrier);
    let first_thread = std::thread::spawn(move || {
        first_barrier.wait();
        first_store.activate(&first)
    });
    let second_store = store.clone();
    let second_barrier = Arc::clone(&barrier);
    let second_thread = std::thread::spawn(move || {
        second_barrier.wait();
        second_store.activate(&second)
    });
    barrier.wait();

    let first_result = first_thread
        .join()
        .unwrap_or_else(|_| panic!("first activation thread must not panic"));
    let second_result = second_thread
        .join()
        .unwrap_or_else(|_| panic!("second activation thread must not panic"));
    match (first_result, second_result) {
        (Ok(first), Err(ReleaseError::ReleaseStoreBusy)) => {
            assert_eq!(first.commit(), FIRST_COMMIT);
        }
        (Err(ReleaseError::ReleaseStoreBusy), Ok(second)) => {
            assert_eq!(second.commit(), SECOND_COMMIT);
        }
        (Ok(first), Ok(second)) => {
            assert_eq!(first.commit(), FIRST_COMMIT);
            assert_eq!(second.commit(), SECOND_COMMIT);
        }
        (first, second) => {
            panic!("concurrent activation results are invalid: {first:?}, {second:?}");
        }
    }
}

#[test]
fn root_manifest_does_not_consume_generated_entry_budget() {
    let root = TestDirectory::new();
    let policy = ReleasePolicy {
        maximum_entries: 1,
        maximum_file_bytes: 64,
        maximum_total_bytes: 64,
    };
    let store = ReleaseStore::open(root.0.join("releases"), policy)
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    let build = build(&store, batch(FIRST_BATCH), "fictional output\n");

    let release = store
        .prepare(build, FIRST_COMMIT, timestamp("2026-07-31T04:00:00Z"))
        .unwrap_or_else(|error| panic!("manifest must not consume the site entry limit: {error}"));
    store
        .activate(&release)
        .unwrap_or_else(|error| panic!("bounded release must activate: {error}"));
}

#[test]
fn enforces_generated_output_limits() {
    let root = TestDirectory::new();
    let policy = ReleasePolicy {
        maximum_entries: 2,
        maximum_file_bytes: 8,
        maximum_total_bytes: 16,
    };
    let store = ReleaseStore::open(root.0.join("releases"), policy)
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    let build = build(
        &store,
        batch(FIRST_BATCH),
        "fictional output exceeds limit\n",
    );

    assert!(matches!(
        store.prepare(build, FIRST_COMMIT, timestamp("2026-07-31T04:00:00Z")),
        Err(ReleaseError::OutputTooLarge)
    ));
}

#[test]
fn enforces_aggregate_generated_output_limit() {
    let root = TestDirectory::new();
    let policy = ReleasePolicy {
        maximum_entries: 2,
        maximum_file_bytes: 16,
        maximum_total_bytes: 20,
    };
    let store = ReleaseStore::open(root.0.join("releases"), policy)
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    let output = store
        .begin_build(batch(FIRST_BATCH))
        .unwrap_or_else(|error| panic!("release build must begin: {error}"));
    fs::write(output.path().join("index.html"), "123456789012")
        .unwrap_or_else(|error| panic!("first output file must be written: {error}"));
    fs::write(output.path().join("page.html"), "123456789012")
        .unwrap_or_else(|error| panic!("second output file must be written: {error}"));
    assert!(matches!(
        store.prepare(
            built(output),
            FIRST_COMMIT,
            timestamp("2026-07-31T04:00:00Z"),
        ),
        Err(ReleaseError::OutputTooLarge)
    ));
}

#[test]
fn generated_output_depth_is_bounded() {
    let root = TestDirectory::new();
    let within_limit = root.0.join("within-limit");
    fs::create_dir(&within_limit)
        .unwrap_or_else(|error| panic!("bounded output root must be created: {error}"));
    let mut directory = within_limit.clone();
    for depth in 0..MAXIMUM_RELEASE_TREE_DEPTH {
        directory = directory.join(format!("level-{depth:02}"));
        fs::create_dir(&directory)
            .unwrap_or_else(|error| panic!("bounded output directory must be created: {error}"));
    }
    fs::write(directory.join("index.html"), "fictional output\n")
        .unwrap_or_else(|error| panic!("bounded output file must be written: {error}"));
    validate_release_tree(&within_limit, ReleasePolicy::default(), false)
        .unwrap_or_else(|error| panic!("output at the depth limit must validate: {error}"));

    let beyond_limit = root.0.join("beyond-limit");
    fs::create_dir(&beyond_limit)
        .unwrap_or_else(|error| panic!("deep output root must be created: {error}"));
    let mut directory = beyond_limit.clone();
    for depth in 0..=MAXIMUM_RELEASE_TREE_DEPTH {
        directory = directory.join(format!("level-{depth:02}"));
        fs::create_dir(&directory)
            .unwrap_or_else(|error| panic!("deep output directory must be created: {error}"));
    }
    fs::write(directory.join("index.html"), "fictional output\n")
        .unwrap_or_else(|error| panic!("deep output file must be written: {error}"));
    assert!(matches!(
        validate_release_tree(&beyond_limit, ReleasePolicy::default(), false),
        Err(ReleaseError::OutputTooLarge)
    ));
}

#[test]
fn rejects_replaced_fixed_release_directories() {
    let root = TestDirectory::new();
    let releases = root.0.join("releases");
    let store = ReleaseStore::open(&releases, ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    let by_id = releases.join("by-id");
    fs::rename(&by_id, releases.join("detached-by-id"))
        .unwrap_or_else(|error| panic!("by-id directory must be moved aside: {error}"));
    fs::create_dir(&by_id)
        .unwrap_or_else(|error| panic!("replacement by-id directory must be created: {error}"));

    assert!(matches!(
        store.begin_build(batch(FIRST_BATCH)),
        Err(ReleaseError::StorageBindingMismatch)
    ));
}

#[test]
fn missing_binding_does_not_rebind_a_populated_store() {
    let root = TestDirectory::new();
    let releases = root.0.join("releases");
    {
        let store = ReleaseStore::open(&releases, ReleasePolicy::default())
            .unwrap_or_else(|error| panic!("release store must open: {error}"));
        store
            .prepare(
                build(&store, batch(FIRST_BATCH), "fictional output\n"),
                FIRST_COMMIT,
                timestamp("2026-07-31T04:00:00Z"),
            )
            .unwrap_or_else(|error| panic!("release must prepare: {error}"));
    }
    fs::remove_file(releases.join(BINDING_FILE))
        .unwrap_or_else(|error| panic!("binding fixture must be removed: {error}"));

    assert!(matches!(
        ReleaseStore::open(&releases, ReleasePolicy::default()),
        Err(ReleaseError::StorageBindingMismatch)
    ));
}

#[cfg(unix)]
#[test]
fn legacy_binding_migrates_by_stable_inode_identity() {
    use std::os::unix::fs::MetadataExt;

    let root = TestDirectory::new();
    let releases = root.0.join("releases");
    let store = ReleaseStore::open(&releases, ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    let mut legacy = b"agent-knowledge-release-store-v4\0".to_vec();
    legacy.extend_from_slice(store.configured_root.as_os_str().as_encoded_bytes());
    for handle in [
        store.root_handle.as_ref(),
        store.by_id.handle.as_ref(),
        store.by_commit.handle.as_ref(),
        store.by_batch.handle.as_ref(),
        store.cleanup_intent.handle.as_ref(),
        store.staging.handle.as_ref(),
    ] {
        legacy.push(0);
        legacy.extend_from_slice(&u64::MAX.to_le_bytes());
        legacy.extend_from_slice(
            &handle
                .metadata()
                .unwrap_or_else(|error| panic!("pinned directory must have metadata: {error}"))
                .ino()
                .to_le_bytes(),
        );
    }
    fs::write(releases.join(LEGACY_BINDING_FILE), legacy)
        .unwrap_or_else(|error| panic!("legacy binding fixture must be written: {error}"));
    fs::remove_file(releases.join(BINDING_FILE))
        .unwrap_or_else(|error| panic!("current binding fixture must be removed: {error}"));
    drop(store);

    ReleaseStore::open(&releases, ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("legacy binding must migrate by inode: {error}"));
    assert!(releases.join(BINDING_FILE).is_file());
}

use std::fs;
use std::io;
use std::path::PathBuf;

use agent_knowledge_core::BatchId;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use ulid::Ulid;

use super::{ReleaseError, ReleasePolicy, ReleaseStore};

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

fn build(store: &ReleaseStore, batch_id: BatchId, body: &str) {
    let output = store
        .begin_build(batch_id)
        .unwrap_or_else(|error| panic!("release build must begin: {error}"));
    fs::write(output.path().join("index.html"), body)
        .unwrap_or_else(|error| panic!("release output must be written: {error}"));
}

#[test]
fn prepares_immutable_releases_and_atomically_changes_current() {
    let root = TestDirectory::new();
    let releases = root.0.join("releases");
    let store = ReleaseStore::open(&releases, ReleasePolicy::default())
        .unwrap_or_else(|error| panic!("release store must open: {error}"));
    build(
        &store,
        batch(FIRST_BATCH),
        "<p>first fictional release</p>\n",
    );
    let first = store
        .prepare(
            batch(FIRST_BATCH),
            FIRST_COMMIT,
            timestamp("2026-07-31T04:00:00Z"),
        )
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

    build(
        &store,
        batch(SECOND_BATCH),
        "<p>second fictional release</p>\n",
    );
    let second = store
        .prepare(
            batch(SECOND_BATCH),
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
    build(&store, batch(FIRST_BATCH), "fictional output\n");
    let release = store
        .prepare(
            batch(FIRST_BATCH),
            FIRST_COMMIT,
            timestamp("2026-07-31T04:00:00Z"),
        )
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
    build(&store, batch(FIRST_BATCH), "fictional output\n");
    let created_at = timestamp("2026-07-31T04:00:00Z");
    let first = store
        .prepare(batch(FIRST_BATCH), FIRST_COMMIT, created_at)
        .unwrap_or_else(|error| panic!("release must prepare: {error}"));
    let recovered = store
        .prepare(batch(FIRST_BATCH), FIRST_COMMIT, created_at)
        .unwrap_or_else(|error| panic!("renamed release must recover: {error}"));

    assert_eq!(first, recovered);
}

#[test]
fn recovers_a_prepared_release_after_reopening_the_store() {
    let root = TestDirectory::new();
    let releases = root.0.join("releases");
    let prepared = {
        let store = ReleaseStore::open(&releases, ReleasePolicy::default())
            .unwrap_or_else(|error| panic!("release store must open: {error}"));
        build(&store, batch(FIRST_BATCH), "fictional output\n");
        store
            .prepare(
                batch(FIRST_BATCH),
                FIRST_COMMIT,
                timestamp("2026-07-31T04:00:00Z"),
            )
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

#[cfg(target_os = "linux")]
#[test]
fn build_directory_keeps_its_staging_lease_after_store_drop() {
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
            batch(FIRST_BATCH),
            FIRST_COMMIT,
            timestamp("2026-07-31T04:00:00Z")
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
    build(&store, batch(FIRST_BATCH), "first fictional output\n");
    let first = store
        .prepare(
            batch(FIRST_BATCH),
            FIRST_COMMIT,
            timestamp("2026-07-31T04:00:00Z"),
        )
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
            batch(SECOND_BATCH),
            SECOND_COMMIT,
            timestamp("2026-07-31T04:05:00Z")
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
    build(&store, batch(FIRST_BATCH), "first fictional output\n");
    let first = store
        .prepare(
            batch(FIRST_BATCH),
            FIRST_COMMIT,
            timestamp("2026-07-31T04:00:00Z"),
        )
        .unwrap_or_else(|error| panic!("first release must prepare: {error}"));
    store
        .activate(&first)
        .unwrap_or_else(|error| panic!("first release must activate: {error}"));

    build(&store, batch(SECOND_BATCH), "second fictional output\n");
    let second = store
        .prepare(
            batch(SECOND_BATCH),
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
    build(&store, batch(FIRST_BATCH), "fictional output\n");

    let release = store
        .prepare(
            batch(FIRST_BATCH),
            FIRST_COMMIT,
            timestamp("2026-07-31T04:00:00Z"),
        )
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
    build(
        &store,
        batch(FIRST_BATCH),
        "fictional output exceeds limit\n",
    );

    assert!(matches!(
        store.prepare(
            batch(FIRST_BATCH),
            FIRST_COMMIT,
            timestamp("2026-07-31T04:00:00Z")
        ),
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

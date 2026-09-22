use super::*;
use agent_knowledge_core::DocumentId;
use agent_knowledge_protocol::{InspectQuery, InspectRequest, Inspection};

const ID: &str = "01K00000000000000000000001";
const PATH: &str =
    "projects/fictional-project/runbooks/2026-07-31-01K00000000000000000000001/index.md";
fn id() -> DocumentId {
    ID.parse().unwrap_or_else(|e| panic!("fixture ID: {e}"))
}
fn request(query: InspectQuery) -> InspectRequest {
    InspectRequest {
        protocol_version: 1,
        query,
    }
}
fn inspect(gateway: &ReadGateway, query: InspectQuery) -> Inspection {
    gateway
        .inspect(&request(query))
        .unwrap_or_else(|e| panic!("inspection: {e}"))
        .result
}
fn commit(root: &TestDirectory) {
    let content = root.path().join("content");
    run_git(Some(&content), &["add", "."]);
    run_git(
        Some(&content),
        &[
            "-c",
            "user.name=Fictional Writer",
            "-c",
            "user.email=writer@fictional.invalid",
            "commit",
            "-m",
            "Update fictional knowledge",
        ],
    );
}
fn head(gateway: &ReadGateway) -> String {
    gateway
        .get(GetRequest::new(id()))
        .unwrap_or_else(|e| panic!("get: {e}"))
        .commit
}
fn search(query: &str) -> InspectQuery {
    InspectQuery::SearchExcerpts {
        query: query.into(),
        filter: ReadFilterRequest::default(),
        maximum_results: 10,
        excerpt_characters: 30,
    }
}

#[test]
fn excerpts_preserve_rank_commit_and_distinguish_tag_matches() {
    for indexed in [false, true] {
        let root = TestDirectory::create();
        initialize_committed_content(&root);
        let config = if indexed {
            publish_search_index(&root);
            settings(&root)
        } else {
            settings_without_search_index(&root)
        };
        let gateway =
            ReadGateway::open_until(&config, None).unwrap_or_else(|e| panic!("open: {e}"));
        let Inspection::SearchExcerpts { commit, hits } = inspect(&gateway, search("safely"))
        else {
            panic!("search response")
        };
        assert_eq!(commit, head(&gateway));
        assert_eq!(hits.len(), 1);
        assert!(
            hits[0]
                .excerpts
                .iter()
                .any(|e| e.field == "body" && e.text.contains("safely"))
        );
        assert!(
            hits[0]
                .excerpts
                .iter()
                .all(|e| e.text.chars().count() <= 30)
        );
        let Inspection::SearchExcerpts { hits, .. } = inspect(&gateway, search("operations"))
        else {
            panic!("search response")
        };
        assert!(hits[0].excerpts.iter().any(|e| e.field == "tags"));
        assert!(!hits[0].excerpts.iter().any(|e| e.field == "body"));
    }
}

#[test]
fn excerpt_queries_keep_index_syntax_and_fail_closed_on_stale_index() {
    let root = TestDirectory::create();
    initialize_committed_content(&root);
    publish_search_index(&root);
    let gateway =
        ReadGateway::open_until(&settings(&root), None).unwrap_or_else(|e| panic!("open: {e}"));
    let Inspection::SearchExcerpts { hits, .. } =
        inspect(&gateway, search("body:\"fictional service\""))
    else {
        panic!("search response")
    };
    assert_eq!(hits.len(), 1);
    assert!(hits[0].excerpts.iter().all(|e| e.field == "body"));
    let path = root.path().join("content").join(PATH);
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read: {e}"));
    fs::write(path, text.replace("safely", "carefully")).unwrap_or_else(|e| panic!("write: {e}"));
    commit(&root);
    assert!(matches!(
        gateway.inspect(&request(search("service"))),
        Err(GatewayError::SearchIndexUnavailable)
    ));
}

#[test]
fn context_uses_one_snapshot_prioritizes_index_and_reports_unicode_truncation() {
    let root = TestDirectory::create();
    let gateway = read_gateway(&root);
    let text = "---\nschema_version: 1\ndocument_id: 01K00000000000000000000003\ntitle: Fictional project\ncreated: 2026-07-31T03:50:00Z\nrequest_id: 01K00000000000000000000004\ntags: []\nstatus: active\n---\n日本語のプロジェクト概要です。\n";
    fs::write(
        root.path()
            .join("content/projects/fictional-project/index.md"),
        text,
    )
    .unwrap_or_else(|e| panic!("write: {e}"));
    commit(&root);
    let query = InspectQuery::Context {
        project: "fictional-project"
            .parse()
            .unwrap_or_else(|e| panic!("project: {e}")),
        query: None,
        maximum_documents: 5,
        maximum_characters: 5,
    };
    let Inspection::Context {
        commit,
        documents,
        additional,
        truncated,
    } = inspect(&gateway, query)
    else {
        panic!("context response")
    };
    assert_eq!(commit, head(&gateway));
    assert!(truncated);
    assert_eq!(documents.len(), 1);
    assert_eq!(documents[0].reason, "project_index");
    assert_eq!(documents[0].body.chars().count(), 5);
    assert!(documents[0].truncated);
    assert_eq!(additional.len(), 1);
    assert_eq!(additional[0].metadata.document_id, id());
}

#[test]
fn context_deduplicates_guidance_and_excludes_deprecated_documents() {
    let root = TestDirectory::create();
    let gateway = read_gateway(&root);
    let query = || InspectQuery::Context {
        project: "fictional-project"
            .parse()
            .unwrap_or_else(|e| panic!("project: {e}")),
        query: Some("service".into()),
        maximum_documents: 10,
        maximum_characters: 1000,
    };
    let Inspection::Context {
        documents,
        truncated,
        ..
    } = inspect(&gateway, query())
    else {
        panic!("context response")
    };
    assert_eq!(documents.len(), 1);
    assert_eq!(documents[0].reason, "related_guidance");
    assert!(!truncated);
    let path = root.path().join("content").join(PATH);
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read: {e}"));
    fs::write(path, text.replace("status: active", "status: deprecated"))
        .unwrap_or_else(|e| panic!("write: {e}"));
    commit(&root);
    let Inspection::Context { documents, .. } = inspect(&gateway, query()) else {
        panic!("context response")
    };
    assert!(documents.is_empty());
}

#[test]
fn history_follows_move_archive_and_pages_at_a_fixed_anchor() {
    let root = TestDirectory::create();
    let gateway = read_gateway(&root);
    let initial = head(&gateway);
    let content = root.path().join("content");
    let old = content.join(PATH);
    let changed = fs::read_to_string(&old)
        .unwrap_or_else(|e| panic!("read: {e}"))
        .replace("safely", "carefully");
    fs::write(&old, changed).unwrap_or_else(|e| panic!("write: {e}"));
    commit(&root);
    let updated = head(&gateway);
    let archived = content.join(PATH.replace("/runbooks/", "/archive/runbooks/"));
    fs::create_dir_all(archived.parent().unwrap_or_else(|| panic!("parent")))
        .unwrap_or_else(|e| panic!("mkdir: {e}"));
    fs::rename(&old, &archived).unwrap_or_else(|e| panic!("move: {e}"));
    let text = fs::read_to_string(&archived).unwrap_or_else(|e| panic!("read: {e}"));
    fs::write(
        &archived,
        text.replace("status: active", "status: archived"),
    )
    .unwrap_or_else(|e| panic!("write: {e}"));
    // Move the attachment with its document bundle.
    fs::rename(
        old.with_file_name("procedure.json"),
        archived.with_file_name("procedure.json"),
    )
    .unwrap_or_else(|e| panic!("move attachment: {e}"));
    commit(&root);
    let archived_commit = head(&gateway);
    let Inspection::History {
        anchor_commit,
        entries,
        next_cursor,
    } = inspect(
        &gateway,
        InspectQuery::History {
            document_id: id(),
            anchor_commit: None,
            cursor: None,
            maximum_results: 1,
        },
    )
    else {
        panic!("history response")
    };
    assert_eq!(anchor_commit, archived_commit);
    assert_eq!(entries.len(), 1);
    assert!(entries[0].document.archived);
    assert_eq!(next_cursor.as_deref(), Some(updated.as_str()));
    // Publication after page one must not move the original pagination anchor.
    let text = fs::read_to_string(&archived).unwrap_or_else(|e| panic!("read: {e}"));
    fs::write(&archived, text.replace("carefully", "calmly"))
        .unwrap_or_else(|e| panic!("write: {e}"));
    commit(&root);
    assert_ne!(head(&gateway), anchor_commit);

    let Inspection::History {
        entries,
        next_cursor,
        ..
    } = inspect(
        &gateway,
        InspectQuery::History {
            document_id: id(),
            anchor_commit: Some(anchor_commit),
            cursor: next_cursor,
            maximum_results: 10,
        },
    )
    else {
        panic!("history response")
    };
    assert_eq!(
        entries
            .iter()
            .map(|e| e.commit.as_str())
            .collect::<Vec<_>>(),
        [updated.as_str(), initial.as_str()]
    );
    assert!(next_cursor.is_none());
    let Inspection::GetAt { document, .. } = inspect(
        &gateway,
        InspectQuery::GetAt {
            document_id: id(),
            commit: initial.clone(),
        },
    ) else {
        panic!("get response")
    };
    assert!(document.markdown.contains("safely"));
    assert!(!document.summary.archived);
    let Inspection::Diff {
        before,
        after,
        body,
        ..
    } = inspect(
        &gateway,
        InspectQuery::Diff {
            document_id: id(),
            from_commit: initial,
            to_commit: archived_commit,
        },
    )
    else {
        panic!("diff response")
    };
    assert!(!before.archived && after.archived);
    assert!(body.removed.contains("safely"));
    assert!(body.added.contains("carefully"));
    assert!(!body.removed.contains("schema_version"));
}

#[test]
fn rejects_unpublished_commits_expressions_and_invalid_limits() {
    let root = TestDirectory::create();
    let gateway = read_gateway(&root);
    for commit in [
        "HEAD",
        "main~1",
        "--all",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    ] {
        let error = gateway
            .inspect(&request(InspectQuery::GetAt {
                document_id: id(),
                commit: commit.into(),
            }))
            .err()
            .unwrap_or_else(|| panic!("request must fail"));
        assert_eq!(error.error_code(), ErrorCode::InvalidRequest);
    }
    let mut query = search("service");
    if let InspectQuery::SearchExcerpts {
        excerpt_characters, ..
    } = &mut query
    {
        *excerpt_characters = 0;
    }
    assert_eq!(
        gateway
            .inspect(&request(query))
            .err()
            .unwrap_or_else(|| panic!("request must fail"))
            .error_code(),
        ErrorCode::LimitExceeded
    );
    // Import a real, unpublished commit without advancing the official branch.
    let seed = root.path().join("seed");
    fs::write(seed.join("fictional-draft.txt"), "unpublished")
        .unwrap_or_else(|e| panic!("write: {e}"));
    run_git(Some(&seed), &["add", "."]);
    run_git(Some(&seed), &["commit", "-m", "Unpublished fictional work"]);
    let draft = Command::new("git")
        .current_dir(&seed)
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap_or_else(|e| panic!("git: {e}"));
    let draft = String::from_utf8(draft.stdout)
        .unwrap_or_else(|e| panic!("utf8: {e}"))
        .trim()
        .to_owned();
    run_git(
        None,
        &[
            "--git-dir",
            path_text(&root.path().join("repository")),
            "fetch",
            path_text(&seed),
            "main:refs/heads/fictional-draft",
        ],
    );
    assert_eq!(
        gateway
            .inspect(&request(InspectQuery::GetAt {
                document_id: id(),
                commit: draft
            }))
            .err()
            .unwrap_or_else(|| panic!("request must fail"))
            .error_code(),
        ErrorCode::InvalidRequest
    );
}

#[test]
fn linear_excerpts_map_expanded_lowercase_back_to_original_unicode() {
    let root = TestDirectory::create();
    let gateway = read_gateway(&root);
    let path = root.path().join("content").join(PATH);
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read: {e}"));
    fs::write(
        path,
        text.replace("Restart the fictional service safely.", "İx"),
    )
    .unwrap_or_else(|e| panic!("write: {e}"));
    commit(&root);
    let query = InspectQuery::SearchExcerpts {
        query: "\u{307}".into(),
        filter: ReadFilterRequest::default(),
        maximum_results: 10,
        excerpt_characters: 1,
    };
    let Inspection::SearchExcerpts { hits, .. } = inspect(&gateway, query) else {
        panic!("search response")
    };
    assert_eq!(hits[0].excerpts[0].field, "body");
    assert_eq!(hits[0].excerpts[0].text, "İ");
}

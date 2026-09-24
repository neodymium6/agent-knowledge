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
        text.replace(
            "日本語のプロジェクト概要です。",
            &"日本語のプロジェクト概要です。".repeat(20),
        ),
    )
    .unwrap_or_else(|e| panic!("write: {e}"));
    commit(&root);
    let query = InspectQuery::Context {
        project: "fictional-project"
            .parse()
            .unwrap_or_else(|e| panic!("project: {e}")),
        query: None,
        maximum_documents: 5,
        maximum_characters: 80,
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
    assert_eq!(documents[0].body.chars().count(), 80);
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

fn fixture_document(
    root: &TestDirectory,
    number: usize,
    kind: &str,
    day: usize,
    body: &str,
) -> DocumentId {
    let identifier = format!("01K{number:023}");
    let date = format!("2026-09-{day:02}");
    let location = match kind {
        "index" => "projects/fictional-project/index.md".to_owned(),
        "logs" => {
            format!("projects/fictional-project/logs/2026/09/{day:02}/120000-{identifier}/index.md")
        }
        _ => format!("projects/fictional-project/{kind}/{date}-{identifier}/index.md"),
    };
    let path = root.path().join("content").join(location);
    fs::create_dir_all(path.parent().unwrap_or_else(|| panic!("parent")))
        .unwrap_or_else(|e| panic!("mkdir: {e}"));
    let log_fields = if kind == "logs" {
        "node: fictional-node\nagent: fictional-agent\nsession: 01K00000000000000000000999\n"
    } else {
        ""
    };
    fs::write(path, format!("---\nschema_version: 1\ndocument_id: {identifier}\ntitle: Fictional service record\ncreated: {date}T12:00:00Z\nrequest_id: 01K00000000000000000000998\n{log_fields}tags: []\nstatus: active\n---\n{body}")).unwrap_or_else(|e| panic!("write: {e}"));
    identifier.parse().unwrap_or_else(|e| panic!("ID: {e}"))
}

#[test]
fn context_omits_tiny_fragments_but_preserves_complete_short_bodies() {
    for indexed in [false, true] {
        let root = TestDirectory::create();
        initialize_committed_content(&root);
        let index = fixture_document(&root, 10, "index", 1, &"界".repeat(80));
        let short = fixture_document(&root, 11, "references", 2, "ok");
        commit(&root);
        if indexed {
            publish_search_index(&root);
        }
        let config = if indexed {
            settings(&root)
        } else {
            settings_without_search_index(&root)
        };
        let gateway =
            ReadGateway::open_until(&config, None).unwrap_or_else(|e| panic!("open: {e}"));
        for remaining in 1..=4 {
            let Inspection::Context {
                documents,
                additional,
                truncated,
                commit,
                ..
            } = inspect(
                &gateway,
                InspectQuery::Context {
                    project: "fictional-project"
                        .parse()
                        .unwrap_or_else(|e| panic!("project: {e}")),
                    query: Some("service".into()),
                    maximum_documents: 4,
                    maximum_characters: 80 + remaining,
                },
            )
            else {
                panic!("context");
            };
            assert_eq!(commit, head(&gateway));
            assert!(truncated);
            assert_eq!(documents[0].document.metadata.document_id, index);
            assert!(documents.iter().all(|d| !d.truncated));
            assert!(additional.iter().any(|d| d.metadata.document_id == id()));
            assert_eq!(
                documents
                    .iter()
                    .any(|d| d.document.metadata.document_id == short && d.body == "ok"),
                remaining >= 2
            );
            assert!(
                documents
                    .iter()
                    .map(|d| d.body.chars().count())
                    .sum::<usize>()
                    <= 80 + remaining
            );
        }
    }
}

#[test]
fn balanced_context_keeps_guidance_and_requested_recent_logs_despite_long_index_and_old_matches() {
    for indexed in [false, true] {
        let root = TestDirectory::create();
        initialize_committed_content(&root);
        let index = fixture_document(&root, 10, "index", 1, &"概要".repeat(1500));
        let reference = fixture_document(
            &root,
            20,
            "references",
            1,
            &"backup durable guidance. ".repeat(20),
        );
        for day in 1..=20 {
            fixture_document(&root, 30 + day, "logs", day, &"backup ".repeat(300));
        }
        let recent = fixture_document(
            &root,
            60,
            "logs",
            21,
            "The newest fictional observation has different wording.",
        );
        commit(&root);
        if indexed {
            publish_search_index(&root);
        }
        let config = if indexed {
            settings(&root)
        } else {
            settings_without_search_index(&root)
        };
        let gateway =
            ReadGateway::open_until(&config, None).unwrap_or_else(|e| panic!("open: {e}"));
        let query = || InspectQuery::ContextBalanced {
            project: "fictional-project"
                .parse()
                .unwrap_or_else(|e| panic!("project: {e}")),
            query: Some("backup".into()),
            maximum_documents: 3,
            maximum_characters: 600,
            recent_documents: 1,
        };
        let first = inspect(&gateway, query());
        let second = inspect(&gateway, query());
        assert_eq!(
            serde_json::to_value(&first).ok(),
            serde_json::to_value(second).ok()
        );
        let Inspection::Context {
            documents,
            additional,
            commit,
            truncated,
        } = first
        else {
            panic!("context");
        };
        assert_eq!(commit, head(&gateway));
        assert_eq!(
            documents
                .iter()
                .map(|d| d.document.metadata.document_id)
                .collect::<Vec<_>>(),
            [index, recent, reference]
        );
        assert_eq!(documents[1].reason, "recent_observation");
        assert_eq!(documents[2].reason, "related_guidance");
        assert!(documents.iter().all(|d| d.body.chars().count() <= 200));
        assert!(
            documents
                .iter()
                .all(|d| !d.truncated || d.body.chars().count() >= 80)
        );
        assert!(truncated && !additional.is_empty());
        assert!(additional.len() <= 3);
        let Inspection::Context { documents, .. } = inspect(
            &gateway,
            InspectQuery::Context {
                project: "fictional-project"
                    .parse()
                    .unwrap_or_else(|e| panic!("project: {e}")),
                query: Some("backup".into()),
                maximum_documents: 3,
                maximum_characters: 600,
            },
        ) else {
            panic!("context");
        };
        assert_eq!(documents.len(), 1);
        assert_eq!(documents[0].body.chars().count(), 600);
    }
}

#[test]
fn concise_diff_keeps_exact_identities_and_reports_metadata_only_changes() {
    let root = TestDirectory::create();
    let gateway = read_gateway(&root);
    let path = root.path().join("content").join(PATH);
    let original = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read: {e}"));
    let before = head(&gateway);
    fs::write(
        &path,
        original.replace("Fictional restart guide", "Fictional updated guide"),
    )
    .unwrap_or_else(|e| panic!("write: {e}"));
    commit(&root);
    let after = head(&gateway);
    let Inspection::DiffHunks {
        from_commit,
        to_commit,
        before: old,
        after: new,
        body,
    } = inspect(
        &gateway,
        InspectQuery::DiffHunks {
            document_id: id(),
            from_commit: before.clone(),
            to_commit: after.clone(),
            context_lines: 3,
            maximum_hunks: 20,
            maximum_diff_bytes: 64000,
        },
    )
    else {
        panic!("diff");
    };
    assert_eq!(from_commit, before);
    assert_eq!(to_commit, after);
    assert_ne!(old.metadata.title, new.metadata.title);
    assert!(!body.changed && !body.truncated && body.hunks.is_empty());
}

fn project_document(
    root: &TestDirectory,
    project: Option<&str>,
    number: usize,
    text: &str,
    index: bool,
    archived: bool,
) {
    let identifier = format!("01K{number:023}");
    let prefix = match (project, archived) {
        (Some(project), false) => format!("projects/{project}"),
        (Some(project), true) => format!("projects/{project}/archive"),
        (None, false) => "inbox".into(),
        (None, true) => "archive".into(),
    };
    let location = if index {
        format!("{prefix}/index.md")
    } else {
        format!("{prefix}/references/2026-09-24-{identifier}/index.md")
    };
    let path = root.path().join("content").join(location);
    fs::create_dir_all(path.parent().unwrap_or_else(|| panic!("parent")))
        .unwrap_or_else(|e| panic!("mkdir: {e}"));
    let status = if archived { "archived" } else { "active" };
    let tag = if number.is_multiple_of(2) {
        "shared"
    } else {
        "alternate"
    };
    fs::write(path, format!("---\nschema_version: 1\ndocument_id: {identifier}\ntitle: Fictional project overview\ncreated: 2026-09-24T12:00:00Z\nrequest_id: 01K00000000000000000000998\ntags: [{tag}]\nstatus: {status}\n---\n{text}")).unwrap_or_else(|e| panic!("write: {e}"));
}

fn project_query(
    query: Option<&str>,
    documents: bool,
    maximum: usize,
    archived: bool,
) -> InspectQuery {
    use agent_knowledge_protocol::ProjectSearchScope;
    InspectQuery::Projects {
        query: query.map(str::to_owned),
        search_in: if documents {
            ProjectSearchScope::Documents
        } else {
            ProjectSearchScope::Project
        },
        maximum_results: maximum,
        description_characters: 2,
        include_archived: archived,
    }
}

#[test]
fn project_discovery_uses_index_text_and_ranks_complete_document_hit_counts() {
    for indexed in [false, true] {
        let root = TestDirectory::create();
        initialize_committed_content(&root);
        project_document(
            &root,
            Some("fictional-alpha"),
            1000,
            "宇宙研究 and distant galaxies",
            true,
            false,
        );
        for (project, start, count) in [
            ("fictional-alpha", 1010, 2),
            ("fictional-beta", 1020, 3),
            ("fictional-gamma", 1030, 1),
        ] {
            for number in start..start + count {
                project_document(
                    &root,
                    Some(project),
                    number,
                    "needle observation",
                    false,
                    false,
                );
            }
        }
        project_document(
            &root,
            Some("fictional-gamma"),
            1040,
            "needle archived observation",
            false,
            true,
        );
        project_document(
            &root,
            Some("fictional-archived"),
            1042,
            "needle archived only",
            false,
            true,
        );
        project_document(&root, None, 1050, "needle unclassified", false, false);
        commit(&root);
        let config = if indexed {
            publish_search_index(&root);
            settings(&root)
        } else {
            settings_without_search_index(&root)
        };
        let gateway =
            ReadGateway::open_until(&config, None).unwrap_or_else(|e| panic!("open: {e}"));
        let Inspection::Projects {
            commit,
            projects,
            truncated,
        } = inspect(&gateway, project_query(None, false, 1, false))
        else {
            panic!("projects")
        };
        assert_eq!(commit, head(&gateway));
        assert!(truncated);
        assert_eq!(projects[0].project.as_str(), "fictional-alpha");
        assert_eq!(projects[0].description, "宇宙");
        assert!(projects[0].description_truncated);
        assert_eq!(projects[0].document_count, 3);
        assert!(projects[0].matching_documents.is_none());
        // Match the complete index body, including text beyond the returned prefix.
        for query in [
            "GALAXIES",
            "宇宙研究",
            "fictional-alpha",
            "project overview",
        ] {
            let Inspection::Projects { projects, .. } =
                inspect(&gateway, project_query(Some(query), false, 10, false))
            else {
                panic!("projects")
            };
            assert_eq!(projects.len(), 1);
            assert_eq!(projects[0].project.as_str(), "fictional-alpha");
        }
        let Inspection::Projects { projects, .. } =
            inspect(&gateway, project_query(Some("needle"), false, 10, false))
        else {
            panic!("projects")
        };
        assert!(projects.is_empty());
        let Inspection::Projects {
            projects,
            truncated,
            ..
        } = inspect(&gateway, project_query(Some("needle"), true, 1, false))
        else {
            panic!("projects")
        };
        assert!(truncated);
        assert_eq!(projects[0].project.as_str(), "fictional-beta");
        assert_eq!(projects[0].matching_documents, Some(3));
        assert_eq!(projects[0].document_count, 3);
        assert!(projects[0].index.is_none());
        assert!(projects[0].description.is_empty());
        let Inspection::Projects {
            projects,
            truncated,
            ..
        } = inspect(&gateway, project_query(Some("needle"), true, 10, true))
        else {
            panic!("projects")
        };
        assert!(!truncated);
        assert_eq!(
            projects
                .iter()
                .map(|p| (p.project.as_str(), p.matching_documents))
                .collect::<Vec<_>>(),
            [
                ("fictional-beta", Some(3)),
                ("fictional-alpha", Some(2)),
                ("fictional-gamma", Some(2)),
                ("fictional-archived", Some(1))
            ]
        );
        let Inspection::Projects { projects, .. } =
            inspect(&gateway, project_query(None, false, 10, false))
        else {
            panic!("projects")
        };
        assert_eq!(projects.len(), 4); // Inbox and archive-only project are excluded.
        assert!(
            gateway
                .inspect(&request(project_query(None, true, 10, false)))
                .is_err()
        );
        assert!(
            gateway
                .inspect(&request(project_query(Some(" "), false, 10, false)))
                .is_err()
        );
        // A dirty canonical checkout must never produce discovery or hit counts.
        project_document(
            &root,
            Some("fictional-unpublished"),
            1060,
            "needle",
            false,
            false,
        );
        assert!(
            gateway
                .inspect(&request(project_query(Some("needle"), true, 10, false)))
                .is_err()
        );
    }
}

#[test]
fn multiple_projects_filter_inside_search_before_global_limit_on_both_backends() {
    for indexed in [false, true] {
        let root = TestDirectory::create();
        initialize_committed_content(&root);
        for (project, number) in [
            ("fictional-alpha", 2000),
            ("fictional-beta", 2001),
            ("fictional-gamma", 2002),
        ] {
            project_document(
                &root,
                Some(project),
                number,
                "needle observation",
                false,
                false,
            );
        }
        project_document(
            &root,
            Some("fictional-alpha"),
            2010,
            "needle archived",
            false,
            true,
        );
        project_document(&root, None, 2020, "needle unclassified", false, false);
        commit(&root);
        let config = if indexed {
            publish_search_index(&root);
            settings(&root)
        } else {
            settings_without_search_index(&root)
        };
        let gateway =
            ReadGateway::open_until(&config, None).unwrap_or_else(|e| panic!("open: {e}"));
        let filter: ReadFilterRequest = serde_json::from_value(
            serde_json::json!({"projects":["fictional-gamma","fictional-alpha"],"tag":"shared"}),
        )
        .unwrap_or_else(|e| panic!("filter: {e}"));
        let result = gateway
            .search(&SearchRequest::new("needle".into(), filter.clone(), 10))
            .unwrap_or_else(|e| panic!("search: {e}"));
        assert_eq!(result.documents.len(), 2);
        let global = gateway
            .search(&SearchRequest::new(
                "needle".into(),
                ReadFilterRequest {
                    tag: Some("shared".into()),
                    ..ReadFilterRequest::default()
                },
                10,
            ))
            .unwrap_or_else(|e| panic!("global search: {e}"));
        let expected = global
            .documents
            .iter()
            .filter(|d| {
                d.project.as_ref().is_some_and(|p| {
                    p.as_str() == "fictional-alpha" || p.as_str() == "fictional-gamma"
                })
            })
            .map(|d| d.metadata.document_id)
            .collect::<Vec<_>>();
        assert_eq!(
            result
                .documents
                .iter()
                .map(|d| d.metadata.document_id)
                .collect::<Vec<_>>(),
            expected
        );

        assert!(result.documents.iter().all(|d| {
            d.project
                .as_ref()
                .is_some_and(|p| p.as_str() == "fictional-alpha" || p.as_str() == "fictional-gamma")
        }));
        assert_eq!(
            gateway
                .list(&ListRequest::new(filter.clone(), 10))
                .unwrap_or_else(|e| panic!("list: {e}"))
                .documents
                .len(),
            2
        );
        assert_eq!(
            gateway
                .recent(&ListRequest::new(filter.clone(), 1))
                .unwrap_or_else(|e| panic!("recent: {e}"))
                .documents
                .len(),
            1
        );
        let Inspection::SearchExcerpts { hits, .. } = inspect(
            &gateway,
            InspectQuery::SearchExcerpts {
                query: "needle".into(),
                filter: filter.clone(),
                maximum_results: 1,
                excerpt_characters: 50,
            },
        ) else {
            panic!("excerpts")
        };
        assert_eq!(hits.len(), 1);
        assert!(hits[0].excerpts.iter().any(|e| e.text.contains("needle")));
        let mut archived = filter.clone();
        archived.include_archived = true;
        assert_eq!(
            gateway
                .search(&SearchRequest::new("needle".into(), archived, 10))
                .unwrap_or_else(|e| panic!("search: {e}"))
                .documents
                .len(),
            3
        );
        let mut absent = filter.clone();
        absent.tag = Some("missing".into());
        assert!(
            gateway
                .search(&SearchRequest::new("needle".into(), absent, 10))
                .unwrap_or_else(|e| panic!("search: {e}"))
                .documents
                .is_empty()
        );
        let only_gamma: ReadFilterRequest = serde_json::from_value(
            serde_json::json!({"projects":["fictional-gamma","fictional-missing"]}),
        )
        .unwrap_or_else(|e| panic!("filter: {e}"));
        let result = gateway
            .search(&SearchRequest::new("needle".into(), only_gamma, 1))
            .unwrap_or_else(|e| panic!("search: {e}"));
        assert_eq!(
            result.documents[0].project.as_ref().map(|p| p.as_str()),
            Some("fictional-gamma")
        );
        for value in [
            serde_json::json!({"projects":[]}),
            serde_json::json!({"projects":["fictional-alpha","fictional-alpha"]}),
            serde_json::json!({"project":"fictional-alpha","projects":["fictional-gamma"]}),
            serde_json::json!({"projects":(0..33).map(|i| format!("fictional-{i}")).collect::<Vec<_>>()}),
        ] {
            let invalid: ReadFilterRequest =
                serde_json::from_value(value).unwrap_or_else(|e| panic!("filter: {e}"));
            assert_eq!(
                gateway
                    .search(&SearchRequest::new("needle".into(), invalid.clone(), 10))
                    .err()
                    .unwrap_or_else(|| panic!("must fail"))
                    .error_code(),
                ErrorCode::InvalidRequest
            );
            assert!(gateway.list(&ListRequest::new(invalid, 10)).is_err());
        }
    }
}

#[test]
fn project_document_counts_fail_on_stale_indexes_and_scan_limits() {
    let root = TestDirectory::create();
    initialize_committed_content(&root);
    project_document(&root, Some("fictional-alpha"), 3000, "needle", true, false);
    commit(&root);
    publish_search_index(&root);
    let gateway =
        ReadGateway::open_until(&settings(&root), None).unwrap_or_else(|e| panic!("open: {e}"));
    project_document(&root, Some("fictional-alpha"), 3001, "needle", false, false);
    commit(&root);
    assert!(
        gateway
            .inspect(&request(project_query(Some("needle"), true, 10, false)))
            .is_err()
    );
    // Discovery by index text does not depend on the stale derived search index.
    let Inspection::Projects { projects, .. } =
        inspect(&gateway, project_query(Some("needle"), false, 10, false))
    else {
        panic!("projects")
    };
    assert_eq!(projects[0].document_count, 2);
    let yaml = format!(
        "schema_version: 4\nidentity:\n  gateway_uid: 61001\nstorage:\n  queue_socket: {}\n  git_directory: {}\n  content_root: {}\nrepository:\n  official_branch: main\nreads:\n  maximum_results: 100\n  maximum_query_characters: 512\n  maximum_index_entries: 100000\n  maximum_index_markdown_bytes: 536870912\n  maximum_search_documents: 1\n  maximum_search_markdown_bytes: 536870912\n  operation_timeout_seconds: 30\n  maximum_response_bytes: 268435456\n  search_metadata:\n    node: true\n    agent: true\n    session: true\n    request_id: true\ntransport:\n  submit_timeout_seconds: 300\n",
        root.path().join("queue").display(),
        root.path().join("repository").display(),
        root.path().join("content").display()
    );
    let config = GatewaySettings::decode(&yaml).unwrap_or_else(|e| panic!("settings: {e}"));
    let gateway = ReadGateway::open_until(&config, None).unwrap_or_else(|e| panic!("open: {e}"));
    assert_eq!(
        gateway
            .inspect(&request(project_query(Some("needle"), true, 1, false)))
            .err()
            .unwrap_or_else(|| panic!("must fail rather than return partial counts"))
            .error_code(),
        ErrorCode::LimitExceeded
    );
    for query in [
        InspectQuery::Projects {
            query: None,
            search_in: agent_knowledge_protocol::ProjectSearchScope::Project,
            maximum_results: 10,
            description_characters: 0,
            include_archived: false,
        },
        project_query(None, false, 0, false),
    ] {
        assert!(gateway.inspect(&request(query)).is_err());
    }
}

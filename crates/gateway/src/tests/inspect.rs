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
fn rejects_invalid_excerpt_limits() {
    let root = TestDirectory::create();
    let gateway = read_gateway(&root);
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

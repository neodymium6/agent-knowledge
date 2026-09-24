use super::*;
use agent_knowledge_protocol::{CURRENT_GATEWAY_PROTOCOL_VERSION, InspectQuery, InspectRequest};

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct ExcerptParameters {
    /// Search expression using the configured search backend.
    query: String,
    #[serde(default)]
    project: Option<String>,
    /// Union of project slugs; mutually exclusive with project.
    #[serde(default)]
    projects: Option<Vec<String>>,
    #[serde(default)]
    tag: Option<String>,
    #[serde(default)]
    session: Option<String>,
    #[serde(default)]
    include_archived: bool,
    /// Maximum hits. Defaults to 10.
    #[serde(default)]
    maximum_results: Option<usize>,
    /// Maximum Unicode characters per field excerpt, 1..2000. Defaults to 300.
    #[serde(default)]
    excerpt_characters: Option<usize>,
}
impl ExcerptParameters {
    pub(super) fn request(self) -> Result<InspectRequest, String> {
        let search = SearchParameters {
            query: self.query,
            project: self.project,
            projects: self.projects,
            tag: self.tag,
            session: self.session,
            include_archived: self.include_archived,
            maximum_results: Some(self.maximum_results.unwrap_or(10)),
        }
        .into_request()?;
        Ok(request(InspectQuery::SearchExcerpts {
            query: search.query,
            filter: search.filter,
            maximum_results: search.maximum_results,
            excerpt_characters: bounded(self.excerpt_characters.unwrap_or(300), 2000)?,
        }))
    }
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct ContextParameters {
    /// Project whose index, guidance and recent records should be selected.
    project: String,
    /// Optional task-related search expression.
    #[serde(default)]
    query: Option<String>,
    /// Maximum selected documents. Defaults to 10.
    #[serde(default)]
    maximum_documents: Option<usize>,
    /// Total Unicode characters in returned bodies, 1..100000. Defaults to 20000.
    #[serde(default)]
    maximum_characters: Option<usize>,
    /// Relevance preserves the original order. Balanced shares the body budget,
    /// reserves recent log slots, and prioritizes durable guidance over logs.
    #[serde(default)]
    selection: ContextSelection,
    /// Reserved recent logs in balanced mode. Defaults to min(2, maximum_documents - 2).
    #[serde(default)]
    recent_documents: Option<usize>,
}
#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum ContextSelection {
    #[default]
    Relevance,
    Balanced,
}
impl ContextParameters {
    pub(super) fn request(self) -> Result<InspectRequest, String> {
        let project = self.project.parse().map_err(|_| "invalid project slug")?;
        let query = self.query;
        let maximum_documents = bounded(self.maximum_documents.unwrap_or(10), MAXIMUM_RESULTS)?;
        let maximum_characters = bounded(self.maximum_characters.unwrap_or(20000), 100000)?;
        Ok(request(match self.selection {
            ContextSelection::Relevance => {
                if self.recent_documents.is_some() {
                    return Err("recent_documents requires balanced selection".into());
                }
                InspectQuery::Context {
                    project,
                    query,
                    maximum_documents,
                    maximum_characters,
                }
            }
            ContextSelection::Balanced => {
                let maximum_recent = maximum_documents.saturating_sub(2);
                let recent_documents = self.recent_documents.unwrap_or(2.min(maximum_recent));
                if recent_documents > maximum_recent {
                    return Err(
                        "recent_documents must leave two document slots for index and guidance"
                            .into(),
                    );
                }
                InspectQuery::ContextBalanced {
                    project,
                    query,
                    maximum_documents,
                    maximum_characters,
                    recent_documents,
                }
            }
        }))
    }
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct HistoryParameters {
    document_id: String,
    /// Reuse the returned anchor_commit to keep pagination on the same snapshot.
    #[serde(default)]
    anchor_commit: Option<String>,
    /// Reuse next_cursor with anchor_commit for the next page, even if entries is empty.
    #[serde(default)]
    cursor: Option<String>,
    /// Maximum changes returned. At most 100 commits are scanned per page. Defaults to 20.
    #[serde(default)]
    maximum_results: Option<usize>,
}
impl HistoryParameters {
    pub(super) fn request(self) -> Result<InspectRequest, String> {
        Ok(request(InspectQuery::History {
            document_id: id(&self.document_id)?,
            anchor_commit: self.anchor_commit,
            cursor: self.cursor,
            maximum_results: bounded(self.maximum_results.unwrap_or(20), MAXIMUM_RESULTS)?,
        }))
    }
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct GetAtParameters {
    document_id: String,
    /// Full lowercase Git commit ID from official history, never a ref or expression.
    commit: String,
}
impl GetAtParameters {
    pub(super) fn request(self) -> Result<InspectRequest, String> {
        Ok(request(InspectQuery::GetAt {
            document_id: id(&self.document_id)?,
            commit: self.commit,
        }))
    }
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct DiffParameters {
    document_id: String,
    /// Full lowercase Git commit ID for the earlier selected state.
    from_commit: String,
    /// Full lowercase Git commit ID for the later selected state.
    to_commit: String,
    /// Contiguous preserves the original response. Hunks returns bounded separate ranges.
    #[serde(default)]
    format: DiffFormat,
    /// Context lines per hunk, 0..20. Defaults to 3 in hunks mode.
    #[serde(default)]
    context_lines: Option<usize>,
    /// Maximum hunks, 1..100. Defaults to 20 in hunks mode.
    #[serde(default)]
    maximum_hunks: Option<usize>,
    /// Maximum encoded body-diff JSON bytes, 256..1000000. Defaults to 64000.
    #[serde(default)]
    maximum_diff_bytes: Option<usize>,
}
#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum DiffFormat {
    #[default]
    Contiguous,
    Hunks,
}
impl DiffParameters {
    pub(super) fn request(self) -> Result<InspectRequest, String> {
        let document_id = id(&self.document_id)?;
        let from_commit = self.from_commit;
        let to_commit = self.to_commit;
        Ok(request(match self.format {
            DiffFormat::Contiguous => {
                if self.context_lines.is_some()
                    || self.maximum_hunks.is_some()
                    || self.maximum_diff_bytes.is_some()
                {
                    return Err("hunk limits require hunks format".into());
                }
                InspectQuery::Diff {
                    document_id,
                    from_commit,
                    to_commit,
                }
            }
            DiffFormat::Hunks => {
                let context_lines = self.context_lines.unwrap_or(3);
                let maximum_hunks = bounded(self.maximum_hunks.unwrap_or(20), 100)?;
                let maximum_diff_bytes = self.maximum_diff_bytes.unwrap_or(64000);
                if context_lines > 20 || !(256..=1_000_000).contains(&maximum_diff_bytes) {
                    return Err(
                        "context_lines must be 0..20 and maximum_diff_bytes 256..1000000".into(),
                    );
                }
                InspectQuery::DiffHunks {
                    document_id,
                    from_commit,
                    to_commit,
                    context_lines,
                    maximum_hunks,
                    maximum_diff_bytes,
                }
            }
        }))
    }
}
fn request(query: InspectQuery) -> InspectRequest {
    InspectRequest {
        protocol_version: CURRENT_GATEWAY_PROTOCOL_VERSION,
        query,
    }
}
fn id(value: &str) -> Result<DocumentId, String> {
    value
        .parse()
        .map_err(|_| "document_id must be a canonical ULID".into())
}
fn bounded(value: usize, maximum: usize) -> Result<usize, String> {
    if value == 0 || value > maximum {
        Err(format!("limit must be between 1 and {maximum}"))
    } else {
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_modes_are_explicit_and_legacy_requests_keep_their_wire_shape() {
        let parameters: ContextParameters =
            serde_json::from_value(serde_json::json!({"project":"fictional-project"}))
                .unwrap_or_else(|e| panic!("parameters: {e}"));
        let wire = serde_json::to_value(
            parameters
                .request()
                .unwrap_or_else(|e| panic!("request: {e}")),
        )
        .unwrap_or_else(|e| panic!("JSON: {e}"));
        assert_eq!(
            wire["query"],
            serde_json::json!({"operation":"context","project":"fictional-project","query":null,"maximum_documents":10,"maximum_characters":20000})
        );
        let parameters: ContextParameters = serde_json::from_value(serde_json::json!({"project":"fictional-project","selection":"balanced","maximum_documents":3})).unwrap_or_else(|e| panic!("parameters: {e}"));
        assert!(matches!(
            parameters
                .request()
                .unwrap_or_else(|e| panic!("request: {e}"))
                .query,
            InspectQuery::ContextBalanced {
                recent_documents: 1,
                ..
            }
        ));
        for json in [
            serde_json::json!({"project":"fictional-project","recent_documents":1}),
            serde_json::json!({"project":"fictional-project","selection":"balanced","recent_documents":3,"maximum_documents":4}),
        ] {
            let parameters: ContextParameters =
                serde_json::from_value(json).unwrap_or_else(|e| panic!("parameters: {e}"));
            assert!(parameters.request().is_err());
        }
        let base = serde_json::json!({"document_id":"01K00000000000000000000001","from_commit":"a".repeat(40),"to_commit":"b".repeat(40)});
        let parameters: DiffParameters =
            serde_json::from_value(base.clone()).unwrap_or_else(|e| panic!("parameters: {e}"));
        let wire = serde_json::to_value(
            parameters
                .request()
                .unwrap_or_else(|e| panic!("request: {e}")),
        )
        .unwrap_or_else(|e| panic!("JSON: {e}"));
        assert_eq!(wire["query"]["operation"], "diff");
        assert_eq!(wire["query"].as_object().map(|o| o.len()), Some(4));
        let mut hunks = base.clone();
        hunks["format"] = "hunks".into();
        hunks["context_lines"] = 0.into();
        let parameters: DiffParameters =
            serde_json::from_value(hunks.clone()).unwrap_or_else(|e| panic!("parameters: {e}"));
        assert!(matches!(
            parameters
                .request()
                .unwrap_or_else(|e| panic!("request: {e}"))
                .query,
            InspectQuery::DiffHunks {
                context_lines: 0,
                ..
            }
        ));
        for (field, value) in [
            ("context_lines", 21),
            ("maximum_hunks", 0),
            ("maximum_diff_bytes", 255),
        ] {
            let mut invalid = hunks.clone();
            invalid[field] = value.into();
            let parameters: DiffParameters =
                serde_json::from_value(invalid).unwrap_or_else(|e| panic!("parameters: {e}"));
            assert!(parameters.request().is_err());
        }
        let mut invalid = base;
        invalid["context_lines"] = 3.into();
        let parameters: DiffParameters =
            serde_json::from_value(invalid).unwrap_or_else(|e| panic!("parameters: {e}"));
        assert!(parameters.request().is_err());
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct ProjectsParameters {
    /// Project searches slug/index text. Documents ranks projects by matching document count and requires query.
    #[serde(default)]
    search_in: ProjectSearchMode,
    /// Project-text substring, or a required backend search expression in documents mode.
    #[serde(default)]
    query: Option<String>,
    /// Maximum returned projects, 1..10000. Defaults to 100.
    #[serde(default)]
    maximum_results: Option<usize>,
    /// Maximum Unicode characters of each index body, 1..2000. Defaults to 300.
    #[serde(default)]
    description_characters: Option<usize>,
    /// Include archived documents when discovering projects and counting documents.
    #[serde(default)]
    include_archived: bool,
}
#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum ProjectSearchMode {
    #[default]
    Project,
    Documents,
}
impl ProjectsParameters {
    pub(super) fn request(self) -> Result<InspectRequest, String> {
        if self.query.as_deref().is_some_and(|q| q.trim().is_empty()) {
            return Err("query must not be empty".into());
        }
        if matches!(self.search_in, ProjectSearchMode::Documents) && self.query.is_none() {
            return Err("documents search requires query".into());
        }
        Ok(request(InspectQuery::Projects {
            query: self.query,
            search_in: match self.search_in {
                ProjectSearchMode::Project => agent_knowledge_protocol::ProjectSearchScope::Project,
                ProjectSearchMode::Documents => {
                    agent_knowledge_protocol::ProjectSearchScope::Documents
                }
            },
            maximum_results: bounded(self.maximum_results.unwrap_or(100), MAXIMUM_RESULTS)?,
            description_characters: bounded(self.description_characters.unwrap_or(300), 2000)?,
            include_archived: self.include_archived,
        }))
    }
}
